// Copyright 2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package asb

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	a "github.com/aerospike/aerospike-client-go/v8"
	particleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
	"github.com/aerospike/backup-go/models"
	"github.com/segmentio/asm/base64"
)

// Encoder contains logic for encoding backup data into the .asb format.
// This is a stateful object that must be created for every backup operation.
type Encoder[T models.TokenConstraint] struct {
	config *EncoderConfig

	firstFileWritten atomic.Bool
	id               atomic.Int64
}

// NewEncoder creates a new Encoder.
func NewEncoder[T models.TokenConstraint](cfg *EncoderConfig) *Encoder[T] {
	return &Encoder[T]{
		config: cfg,
	}
}

// GenerateFilename generates a file name for the given namespace.
func (e *Encoder[T]) GenerateFilename(prefix, suffix string) string {
	return prefix + e.config.Namespace + "_" + strconv.FormatInt(e.id.Add(1), 10) + suffix + ".asb"
}

// EncodeToken encodes a token to the ASB format, writing to the provided buffer.
func (e *Encoder[T]) EncodeToken(token T, w *bytes.Buffer) error {
	t, ok := any(token).(*models.Token)
	if !ok {
		return fmt.Errorf("unsupported token type %T for ASB encoder", token)
	}

	var (
		n   int
		err error
	)

	switch t.Type {
	case models.TokenTypeRecord:
		n, err = recordToASB(e.config.Compact, t.Record, w)
	case models.TokenTypeUDF:
		n, err = udfToASB(t.UDF, w)
	case models.TokenTypeSIndex:
		n, err = sindexToASB(t.SIndex, w)
	case models.TokenTypeInvalid:
		n, err = 0, errors.New("invalid token")
	default:
		n, err = 0, fmt.Errorf("invalid token type: %v", t.Type)
	}

	if err != nil {
		return fmt.Errorf("failed to encode token at byte %d: %w", n, err)
	}

	return nil
}

// GetHeader returns the header of the ASB file as a byte slice.
// The header contains the version, namespace, and first file flag.
func (e *Encoder[T]) GetHeader(_ uint64, isRecords bool) []byte {
	// capacity is arbitrary, just probably enough to avoid reallocations
	buff := bytes.NewBuffer(make([]byte, 0, 1024))

	writeVersionText(e.headerVersion(isRecords), buff)

	writeNamespaceMetaText(e.config.Namespace, buff)

	if !e.firstFileWritten.Swap(true) {
		writeFirstMetaText(buff)
	}

	return buff.Bytes()
}

func (e *Encoder[T]) headerVersion(isRecords bool) string {
	if isRecords {
		return version31.toString()
	}

	return e.config.getVersion().toString()
}

// **** META DATA ****

func writeVersionText(asbVersion string, w *bytes.Buffer) {
	_, _ = writeBytes(w, tokenVersion, space, []byte(asbVersion))
}

func writeNamespaceMetaText(namespace string, w *bytes.Buffer) {
	_, _ = writeBytes(w, metadataSection, space, namespaceToken, space, escapeASB(namespace))
}

func writeFirstMetaText(w *bytes.Buffer) {
	_, _ = writeBytes(w, metadataSection, space, tokenFirst)
}

// **** RECORD ****

func recordToASB(c bool, r *models.Record, w *bytes.Buffer) (int, error) {
	var bytesWritten int

	n, err := keyToASB(r.Key, w)
	bytesWritten += n

	if err != nil {
		return bytesWritten, err
	}

	n, err = writeRecordHeaderGeneration(r.Generation, w)
	bytesWritten += n

	if err != nil {
		return bytesWritten, err
	}

	n, err = writeRecordHeaderExpiration(r.VoidTime, w)
	bytesWritten += n

	if err != nil {
		return bytesWritten, err
	}

	n, err = writeRecordHeaderBinCount(len(r.Bins), w)
	bytesWritten += n

	if err != nil {
		return bytesWritten, err
	}

	n, err = binsToASB(c, r.Bins, w)
	bytesWritten += n

	if err != nil {
		return bytesWritten, err
	}

	return bytesWritten, nil
}

func writeRecordHeaderGeneration(generation uint32, w *bytes.Buffer) (int, error) {
	var numBuf [20]byte
	value := strconv.AppendUint(numBuf[:0], uint64(generation), 10)

	return writeBytes(w, headerGeneration, value)
}

func writeRecordHeaderExpiration(expiration int64, w *bytes.Buffer) (int, error) {
	var numBuf [20]byte
	value := strconv.AppendInt(numBuf[:0], expiration, 10)

	return writeBytes(w, headerExpiration, value)
}

func writeRecordHeaderBinCount(binCount int, w *bytes.Buffer) (int, error) {
	var numBuf [20]byte
	value := strconv.AppendInt(numBuf[:0], int64(binCount), 10)

	return writeBytes(w, headerBinCount, value)
}

func binsToASB(compact bool, bins a.BinMap, w *bytes.Buffer) (int, error) {
	var bytesWritten int

	// NOTE golang's random order map iteration
	// means that any backup files that include
	// multi element bin maps may not be identical
	// over multiple backups even if the data is the same
	for k, v := range bins {
		n, err := binToASB(k, compact, v, w)
		bytesWritten += n

		if err != nil {
			return bytesWritten, err
		}
	}

	return bytesWritten, nil
}

func binToASB(k string, compact bool, v any, w *bytes.Buffer) (int, error) {
	switch v := v.(type) {
	case bool:
		return writeBinBool(k, v, w)
	case int64:
		return writeBinInt(k, v, w)
	case int32:
		return writeBinInt(k, v, w)
	case int16:
		return writeBinInt(k, v, w)
	case int8:
		return writeBinInt(k, v, w)
	case int:
		return writeBinInt(k, v, w)
	case float64:
		return writeBinFloat(k, v, w)
	case string:
		return writeBinString(k, v, w)
	case []byte:
		return writeBinBytes(k, compact, v, w)
	case *a.RawBlobValue:
		return writeRawBlobBin(v, k, compact, w)
	case a.HLLValue:
		return writeBinHLL(k, compact, v, w)
	case a.GeoJSONValue:
		return writeBinGeoJSON(k, v, w)
	case nil:
		return writeBinNil(k, w)
	}

	return 0, fmt.Errorf("unknown bin type: %T, key: %s", v, k)
}

func writeBinBool(name string, v bool, w *bytes.Buffer) (int, error) {
	return writeEscapedNameValueLine(w, binBoolTypePrefix, name, boolToASB(v))
}

type binTypesInt interface {
	int64 | int32 | int16 | int8 | int
}

func writeBinInt[T binTypesInt](name string, v T, w *bytes.Buffer) (int, error) {
	var numBuf [20]byte
	value := strconv.AppendInt(numBuf[:0], int64(v), 10)

	return writeEscapedNameValueLine(w, binIntTypePrefix, name, value)
}

func writeBinFloat(name string, v float64, w *bytes.Buffer) (int, error) {
	var numBuf [32]byte
	value := strconv.AppendFloat(numBuf[:0], v, 'g', -1, 64)

	return writeEscapedNameValueLine(w, binFloatTypePrefix, name, value)
}

func writeBinString(name, v string, w *bytes.Buffer) (int, error) {
	var numBuf [20]byte
	valueLen := strconv.AppendInt(numBuf[:0], int64(len(v)), 10)

	return writeEscapedNameLenStringLine(w, binStringTypePrefix, name, valueLen, v)
}

func writeBinBytes(name string, compact bool, v []byte, w *bytes.Buffer) (int, error) {

	var numBuf [20]byte
	if compact {
		valueLen := strconv.AppendInt(numBuf[:0], int64(len(v)), 10)
		return writeEscapedNameLenValueLine(w, binBytesTypeCompactPrefix, name, valueLen, v)
	}

	valueLen := strconv.AppendInt(numBuf[:0], int64(base64.StdEncoding.EncodedLen(len(v))), 10)
	return writeEscapedNameLenBase64Line(w, binBytesTypePrefix, name, valueLen, v)
}

func writeBinHLL(name string, compact bool, v a.HLLValue, w *bytes.Buffer) (int, error) {
	if compact {
		var numBuf [20]byte
		valueLen := strconv.AppendInt(numBuf[:0], int64(len(v)), 10)
		return writeEscapedNameLenValueLine(w, binHLLTypeCompactPrefix, name, valueLen, v)
	}

	var numBuf [20]byte
	valueLen := strconv.AppendInt(numBuf[:0], int64(base64.StdEncoding.EncodedLen(len(v))), 10)
	return writeEscapedNameLenBase64Line(w, binHLLTypePrefix, name, valueLen, v)
}

func writeBinGeoJSON(name string, v a.GeoJSONValue, w *bytes.Buffer) (int, error) {
	var numBuf [20]byte
	valueLen := strconv.AppendInt(numBuf[:0], int64(len(v)), 10)

	return writeEscapedNameLenStringLine(w, binGeoJSONTypePrefix, name, valueLen, string(v))
}

func writeBinNil(name string, w *bytes.Buffer) (int, error) {
	return writeEscapedNameOnlyLine(w, binNilTypePrefix, name)
}

func writeRawBlobBin(cdt *a.RawBlobValue, name string, compact bool, w *bytes.Buffer) (int, error) {
	switch cdt.ParticleType {
	case particleType.MAP:
		return writeRawMapBin(cdt, name, compact, w)
	case particleType.LIST:
		return writeRawListBin(cdt, name, compact, w)
	default:
		return 0, fmt.Errorf("invalid raw blob bin particle type: %v", cdt.ParticleType)
	}
}

func writeRawMapBin(cdt *a.RawBlobValue, name string, compact bool, w *bytes.Buffer) (int, error) {
	if compact {
		var numBuf [20]byte
		valueLen := strconv.AppendInt(numBuf[:0], int64(len(cdt.Data)), 10)
		return writeEscapedNameLenValueLine(w, binMapTypeCompactPrefix, name, valueLen, cdt.Data)
	}

	var numBuf [20]byte
	valueLen := strconv.AppendInt(numBuf[:0], int64(base64.StdEncoding.EncodedLen(len(cdt.Data))), 10)
	return writeEscapedNameLenBase64Line(w, binMapTypePrefix, name, valueLen, cdt.Data)
}

func writeRawListBin(cdt *a.RawBlobValue, name string, compact bool, w *bytes.Buffer) (int, error) {
	if compact {
		var numBuf [20]byte
		valueLen := strconv.AppendInt(numBuf[:0], int64(len(cdt.Data)), 10)
		return writeEscapedNameLenValueLine(w, binListTypeCompactPrefix, name, valueLen, cdt.Data)
	}

	var numBuf [20]byte
	valueLen := strconv.AppendInt(numBuf[:0], int64(base64.StdEncoding.EncodedLen(len(cdt.Data))), 10)
	return writeEscapedNameLenBase64Line(w, binListTypePrefix, name, valueLen, cdt.Data)
}

func blobBinToASB(val []byte, bytesType byte, name string) []byte {
	// Calculate the total size needed to avoid reallocations
	totalSize := 1 + 1 + len(name) + 1 + len(strconv.Itoa(len(val))) + 1 + len(val) + 1

	// Create a buffer with the calculated capacity
	buf := bytes.NewBuffer(make([]byte, 0, totalSize))

	// Write the data to the buffer
	buf.WriteByte(bytesType)
	buf.Write(space)
	buf.WriteString(name)
	buf.Write(space)
	buf.WriteString(strconv.Itoa(len(val)))
	buf.Write(space)
	buf.Write(val)
	buf.Write(newLine)

	return buf.Bytes()
}

func boolToASB(b bool) []byte {
	if b {
		return trueBytes
	}

	return falseBytes
}

func keyToASB(k *a.Key, w *bytes.Buffer) (int, error) {
	var bytesWritten int

	userKey := k.Value()
	if userKey != nil {
		n, err := userKeyToASB(userKey, w)
		bytesWritten += n

		if err != nil {
			return bytesWritten, err
		}
	}

	n, err := writeRecordNamespace(k.Namespace(), w)
	bytesWritten += n

	if err != nil {
		return bytesWritten, err
	}

	n, err = writeRecordDigest(k.Digest(), w)
	bytesWritten += n

	if err != nil {
		return bytesWritten, err
	}

	if k.SetName() != "" {
		n, err = writeRecordSet(k.SetName(), w)
		bytesWritten += n

		if err != nil {
			return bytesWritten, err
		}
	}

	return bytesWritten, nil
}

// base64BufferPool is a pool of byte slices used for base64 encoding
var base64BufferPool = sync.Pool{
	New: func() any {
		// The buffer size is arbitrary and will be grown if needed
		return make([]byte, 0, 1024)
	},
}

// base64Encode encodes the input bytes using base64 encoding.
// It returns a slice that references a pooled buffer, which must be returned to the pool
// after use by calling returnBase64Buffer.
func base64Encode(v []byte) []byte {
	encodedLen := base64.StdEncoding.EncodedLen(len(v))

	// Get a buffer from the pool
	buf := base64BufferPool.Get().([]byte)

	// Ensure the buffer is large enough
	if cap(buf) < encodedLen {
		// If the buffer is too small, create a new one with sufficient capacity
		buf = make([]byte, encodedLen)
	} else {
		// Otherwise, resize the existing buffer
		buf = buf[:encodedLen]
	}

	// Encode the data
	base64.StdEncoding.Encode(buf, v)

	// Return a slice that references the pooled buffer
	return buf
}

// returnBase64Buffer returns the buffer to the pool.
// This must be called after the buffer returned by base64Encode is no longer needed.
func returnBase64Buffer(buf []byte) {
	// Reset length but keep capacity
	//nolint:staticcheck // We try to decrease allocation, not to make them zero.
	base64BufferPool.Put(buf[:0])
}

func writeBytes(w *bytes.Buffer, data ...[]byte) (int, error) {
	totalBytesWritten, err := writeRawBytes(w, data...)
	if err != nil {
		return totalBytesWritten, err
	}

	n, err := w.Write(newLine)
	if err != nil {
		return totalBytesWritten, err
	}

	return totalBytesWritten + n, nil
}

func writeRawBytes(w *bytes.Buffer, data ...[]byte) (int, error) {
	totalBytesWritten := 0

	for _, d := range data {
		n, err := w.Write(d)
		if err != nil {
			return totalBytesWritten, err
		}

		totalBytesWritten += n
	}

	return totalBytesWritten, nil
}

func writeRecordNamespace(namespace string, w *bytes.Buffer) (int, error) {
	return writeEscapedValueLine(w, namespacePrefix, namespace)
}

func writeRecordDigest(digest []byte, w *bytes.Buffer) (int, error) {
	n, err := writeRawBytes(w, digestPrefix)
	if err != nil {
		return n, err
	}

	encodedN, err := writeBase64ToBuffer(w, digest)
	n += encodedN
	if err != nil {
		return n, err
	}

	newLineN, err := w.Write(newLine)
	n += newLineN

	return n, err
}

func writeRecordSet(setName string, w *bytes.Buffer) (int, error) {
	return writeEscapedValueLine(w, setPrefix, setName)
}

func userKeyToASB(userKey a.Value, w *bytes.Buffer) (int, error) {
	switch v := userKey.(type) {
	case a.IntegerValue:
		return writeUserKeyInt(int(v), w)
	case a.LongValue:
		return writeUserKeyInt(int64(v), w)
	case a.FloatValue:
		return writeUserKeyFloat(float64(v), w)
	case a.StringValue:
		return writeUserKeyString(string(v), w)
	case a.BytesValue:
		return writeUserKeyBytes([]byte(v), w)
	case a.NullValue:
		return 0, nil
	}

	// Fallback for custom Value implementations and compatibility with non-standard key values.
	switch v := userKey.GetObject().(type) {
	// need the repeated int cases to satisfy the generic type checker
	case int64:
		return writeUserKeyInt(v, w)
	case int32:
		return writeUserKeyInt(v, w)
	case int16:
		return writeUserKeyInt(v, w)
	case int8:
		return writeUserKeyInt(v, w)
	case int:
		return writeUserKeyInt(v, w)
	case float64:
		return writeUserKeyFloat(v, w)
	case string:
		return writeUserKeyString(v, w)
	case []byte:
		return writeUserKeyBytes(v, w)
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("invalid user key type: %T", v)
	}
}

type UserKeyTypesInt interface {
	int64 | int32 | int16 | int8 | int
}

func writeUserKeyInt[T UserKeyTypesInt](v T, w *bytes.Buffer) (int, error) {
	var numBuf [20]byte
	value := strconv.AppendInt(numBuf[:0], int64(v), 10)

	return writeBytes(
		w,
		recordHeader,
		space,
		recordHeaderType,
		space,
		headerTypeInt,
		space,
		value,
	)
}

func writeUserKeyFloat(v float64, w *bytes.Buffer) (int, error) {
	var numBuf [32]byte
	value := strconv.AppendFloat(numBuf[:0], v, 'f', -1, 64)

	return writeBytes(
		w,
		recordHeader,
		space,
		recordHeaderType,
		space,
		headerTypeFloat,
		space,
		value,
	)
}

func writeUserKeyString(v string, w *bytes.Buffer) (int, error) {
	var lenBuf [20]byte
	valueLen := strconv.AppendInt(lenBuf[:0], int64(len(v)), 10)

	n, err := writeRawBytes(
		w,
		recordHeader,
		space,
		recordHeaderType,
		space,
		headerTypeString,
		space,
		valueLen,
		space,
	)
	if err != nil {
		return n, err
	}

	valueN, err := w.WriteString(v)

	n += valueN
	if err != nil {
		return n, err
	}

	newLineN, err := w.Write(newLine)
	n += newLineN

	return n, err
}

func writeUserKeyBytes(v []byte, w *bytes.Buffer) (int, error) {
	var lenBuf [20]byte
	valueLen := strconv.AppendInt(lenBuf[:0], int64(base64.StdEncoding.EncodedLen(len(v))), 10)
	n, err := writeRawBytes(w,
		recordHeader,
		space,
		recordHeaderType,
		space,
		headerTypeBytes,
		space,
		valueLen,
		space,
	)
	if err != nil {
		return n, err
	}

	encodedN, err := writeBase64ToBuffer(w, v)
	n += encodedN
	if err != nil {
		return n, err
	}

	newLineN, err := w.Write(newLine)
	n += newLineN

	return n, err
}

var needsEscape = [256]bool{ // control characters
	'\\': true,
	' ':  true,
	'\n': true,
}

func escapeASB(s string) []byte {
	idx := -1

	for i := range len(s) {
		if needsEscape[s[i]] {
			idx = i
			break
		}
	}

	// No escaping needed. Return the string as bytes
	if idx == -1 {
		return []byte(s)
	}

	// We found an escape at 'idx'.
	// Now we start building the result.
	out := make([]byte, 0, len(s)+2)
	out = append(out, s[:idx]...)

	for i := idx; i < len(s); i++ {
		c := s[i]
		if needsEscape[c] {
			out = append(out, '\\')
		}

		out = append(out, c)
	}

	return out
}

func writeEscapedValueLine(w *bytes.Buffer, prefix []byte, value string) (int, error) {
	n, err := writeRawBytes(w, prefix)
	if err != nil {
		return n, err
	}

	valueN, err := writeEscapedASBToWriter(w, value)

	n += valueN
	if err != nil {
		return n, err
	}

	newLineN, err := w.Write(newLine)
	n += newLineN

	return n, err
}

func writeEscapedNameOnlyLine(w *bytes.Buffer, prefix []byte, name string) (int, error) {
	n, err := writeRawBytes(w, prefix)
	if err != nil {
		return n, err
	}

	nameN, err := writeEscapedASBToWriter(w, name)

	n += nameN
	if err != nil {
		return n, err
	}

	newLineN, err := w.Write(newLine)
	n += newLineN

	return n, err
}

func writeEscapedNameValueLine(w *bytes.Buffer, prefix []byte, name string, value []byte) (int, error) {
	n, err := writeRawBytes(w, prefix)
	if err != nil {
		return n, err
	}

	nameN, err := writeEscapedASBToWriter(w, name)

	n += nameN
	if err != nil {
		return n, err
	}

	valueN, err := writeRawBytes(w, space, value)

	n += valueN
	if err != nil {
		return n, err
	}

	newLineN, err := w.Write(newLine)
	n += newLineN

	return n, err
}

func writeEscapedNameLenValueLine(w *bytes.Buffer, prefix []byte, name string, valueLen, value []byte) (int, error) {
	n, err := writeRawBytes(w, prefix)
	if err != nil {
		return n, err
	}

	nameN, err := writeEscapedASBToWriter(w, name)

	n += nameN
	if err != nil {
		return n, err
	}

	valueN, err := writeRawBytes(w, space, valueLen, space, value)

	n += valueN
	if err != nil {
		return n, err
	}

	newLineN, err := w.Write(newLine)
	n += newLineN

	return n, err
}

func writeEscapedNameLenBase64Line(
	w *bytes.Buffer,
	prefix []byte,
	name string,
	valueLen []byte,
	value []byte,
) (int, error) {
	n, err := writeRawBytes(w, prefix)
	if err != nil {
		return n, err
	}

	nameN, err := writeEscapedASBToWriter(w, name)

	n += nameN
	if err != nil {
		return n, err
	}

	valueN, err := writeRawBytes(w, space, valueLen, space)

	n += valueN
	if err != nil {
		return n, err
	}

	encodedN, err := writeBase64ToBuffer(w, value)
	n += encodedN
	if err != nil {
		return n, err
	}

	newLineN, err := w.Write(newLine)
	n += newLineN

	return n, err
}

func writeBase64ToBuffer(w *bytes.Buffer, data []byte) (int, error) {
	encodedLen := base64.StdEncoding.EncodedLen(len(data))
	w.Grow(encodedLen)
	dst := w.AvailableBuffer()
	dst = dst[:encodedLen]
	base64.StdEncoding.Encode(dst, data)

	return w.Write(dst)
}

func writeEscapedNameLenStringLine(
	w *bytes.Buffer,
	prefix []byte,
	name string,
	valueLen []byte,
	value string,
) (int, error) {
	n, err := writeRawBytes(w, prefix)
	if err != nil {
		return n, err
	}

	nameN, err := writeEscapedASBToWriter(w, name)

	n += nameN
	if err != nil {
		return n, err
	}

	valueN, err := writeRawBytes(w, space, valueLen, space)

	n += valueN
	if err != nil {
		return n, err
	}

	valueN, err = w.WriteString(value)

	n += valueN
	if err != nil {
		return n, err
	}

	newLineN, err := w.Write(newLine)
	n += newLineN

	return n, err
}

func writeEscapedASBToWriter(w *bytes.Buffer, s string) (int, error) {
	total := 0
	start := 0

	for i := 0; i < len(s); i++ {
		if !needsEscape[s[i]] {
			continue
		}

		n, err := w.WriteString(s[start:i])

		total += n
		if err != nil {
			return total, err
		}

		n, err = 1, w.WriteByte('\\')

		total += n
		if err != nil {
			return total, err
		}

		n, err = 1, w.WriteByte(s[i])

		total += n
		if err != nil {
			return total, err
		}

		start = i + 1
	}

	n, err := w.WriteString(s[start:])
	total += n

	return total, err
}

// **** SINDEX ****
func sindexToASB(sindex *models.SIndex, w *bytes.Buffer) (int, error) {
	sindexSection := globalSIndex
	// If we have sindex with expression, we need to use the globalSIndexExpression section.
	if sindex.Expression != "" {
		sindexSection = globalSIndexExpression
	}

	// Prepare all parameters
	params := [][]byte{
		globalSection,
		space,
		sindexSection,
		space,
		escapeASB(sindex.Namespace),
		space,
		escapeASB(sindex.Set),
		space,
		escapeASB(sindex.Name),
		space,
		{byte(sindex.IndexType)},
		space,
		[]byte("1"),
		space,
		escapeASB(sindex.Path.BinName),
		space,
		{byte(sindex.Path.BinType)},
	}

	// If there's a B64Context, add it to the parameters
	if sindex.Path.B64Context != "" {
		params = append(params, space, []byte(sindex.Path.B64Context))
	}

	// If there is expression add it to the end.
	if sindex.Expression != "" {
		params = append(params, space, []byte(sindex.Expression))
	}

	// Write all parameters at once
	return writeBytes(w, params...)
}

// **** UDFs ****

func udfToASB(udf *models.UDF, w *bytes.Buffer) (int, error) {
	var lenBuf [20]byte
	contentLen := strconv.AppendInt(lenBuf[:0], int64(len(udf.Content)), 10)

	return writeBytes(
		w,
		globalSection,
		space,
		globalUDF,
		space,
		[]byte{byte(udf.UDFType)},
		space,
		escapeASB(udf.Name),
		space,
		contentLen,
		space,
		udf.Content,
	)
}
