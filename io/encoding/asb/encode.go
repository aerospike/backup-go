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
	"io"
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

	estimatedRecordSize atomic.Uint32
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

// WriteToken encodes a token to the ASB format and writes it directly to the provided writer.
// It returns the number of bytes written and an error if the encoding fails.
// This method is more efficient than EncodeToken as it avoids intermediate buffer allocation.
func (e *Encoder[T]) WriteToken(token T, w io.Writer) (int, error) {
	t, ok := any(token).(*models.Token)
	if !ok {
		return 0, fmt.Errorf("unsupported token type %T for ASB encoder", token)
	}

	var (
		n   int
		err error
	)

	switch t.Type {
	case models.TokenTypeRecord:
		n, err = e.encodeRecord(t.Record, w)
	case models.TokenTypeUDF:
		n, err = e.encodeUDF(t.UDF, w)
	case models.TokenTypeSIndex:
		n, err = e.encodeSIndex(t.SIndex, w)
	case models.TokenTypeInvalid:
		n, err = 0, errors.New("invalid token")
	default:
		n, err = 0, fmt.Errorf("invalid token type: %v", t.Type)
	}

	if err != nil {
		return n, fmt.Errorf("error encoding token at byte %d: %w", n, err)
	}

	// keep smoothed last value
	e.estimatedRecordSize.Store((e.estimatedRecordSize.Load() + uint32(n)) / 2)

	return n, nil
}

// EncodeToken encodes a token to the ASB format.
// It returns a byte slice of the encoded token and an error if the encoding fails.
//
// Deprecated: Use WriteToken instead, which writes directly to an io.Writer
// and avoids intermediate buffer allocation.
func (e *Encoder[T]) EncodeToken(token T) ([]byte, error) {
	t, ok := any(token).(*models.Token)
	if !ok {
		return nil, fmt.Errorf("unsupported token type %T for ASB encoder", token)
	}

	var (
		n   int
		err error
	)

	allocationSize := int(e.estimatedRecordSize.Load() * 11 / 10) // add 10% to be safe
	buff := bytes.NewBuffer(make([]byte, 0, allocationSize))

	switch t.Type {
	case models.TokenTypeRecord:
		n, err = e.encodeRecord(t.Record, buff)
	case models.TokenTypeUDF:
		n, err = e.encodeUDF(t.UDF, buff)
	case models.TokenTypeSIndex:
		n, err = e.encodeSIndex(t.SIndex, buff)
	case models.TokenTypeInvalid:
		n, err = 0, errors.New("invalid token")
	default:
		n, err = 0, fmt.Errorf("invalid token type: %v", t.Type)
	}

	if err != nil {
		return nil, fmt.Errorf("error encoding token at byte %d: %w", n, err)
	}

	// keep smoothed last value
	e.estimatedRecordSize.Store((e.estimatedRecordSize.Load() + uint32(n)) / 2)

	return buff.Bytes(), nil
}

func (e *Encoder[T]) encodeRecord(rec *models.Record, w io.Writer) (int, error) {
	return recordToASB(e.config.Compact, rec, w)
}

func (e *Encoder[T]) encodeUDF(udf *models.UDF, w io.Writer) (int, error) {
	return udfToASB(udf, w)
}

func (e *Encoder[T]) encodeSIndex(sIndex *models.SIndex, w io.Writer) (int, error) {
	return sindexToASB(sIndex, w)
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

func writeVersionText(asbVersion string, w io.Writer) {
	_, _ = writeBytes(w, tokenVersion, space, []byte(asbVersion))
}

func writeNamespaceMetaText(namespace string, w io.Writer) {
	_, _ = writeBytes(w, metadataSection, space, namespaceToken, space, escapeASB(namespace))
}

func writeFirstMetaText(w io.Writer) {
	_, _ = writeBytes(w, metadataSection, space, tokenFirst)
}

// **** RECORD ****

func recordToASB(c bool, r *models.Record, w io.Writer) (int, error) {
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

func writeRecordHeaderGeneration(generation uint32, w io.Writer) (int, error) {
	value := []byte(strconv.FormatUint(uint64(generation), 10))
	return writeBytes(w, headerGeneration, value)
}

func writeRecordHeaderExpiration(expiration int64, w io.Writer) (int, error) {
	return writeBytes(w, headerExpiration, []byte(strconv.FormatInt(expiration, 10)))
}

func writeRecordHeaderBinCount(binCount int, w io.Writer) (int, error) {
	return writeBytes(w, headerBinCount, []byte(strconv.Itoa(binCount)))
}

func binsToASB(compact bool, bins a.BinMap, w io.Writer) (int, error) {
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

func binToASB(k string, c bool, v any, w io.Writer) (int, error) {
	var (
		bytesWritten int
		err          error
	)

	switch v := v.(type) {
	case bool:
		bytesWritten, err = writeBinBool(k, v, w)
	case int64:
		bytesWritten, err = writeBinInt(k, v, w)
	case int32:
		bytesWritten, err = writeBinInt(k, v, w)
	case int16:
		bytesWritten, err = writeBinInt(k, v, w)
	case int8:
		bytesWritten, err = writeBinInt(k, v, w)
	case int:
		bytesWritten, err = writeBinInt(k, v, w)
	case float64:
		bytesWritten, err = writeBinFloat(k, v, w)
	case string:
		bytesWritten, err = writeBinString(k, v, w)
	case []byte:
		bytesWritten, err = writeBinBytes(k, c, v, w)
	case *a.RawBlobValue:
		bytesWritten, err = writeRawBlobBin(v, k, c, w)
	case a.HLLValue:
		bytesWritten, err = writeBinHLL(k, c, v, w)
	case a.GeoJSONValue:
		bytesWritten, err = writeBinGeoJSON(k, v, w)
	case nil:
		bytesWritten, err = writeBinNil(k, w)
	default:
		return bytesWritten, fmt.Errorf("unknown bin type: %T, key: %s", v, k)
	}

	return bytesWritten, err
}

func writeBinBool(name string, v bool, w io.Writer) (int, error) {
	return writeBytes(w, binBoolTypePrefix, escapeASB(name), space, boolToASB(v))
}

type binTypesInt interface {
	int64 | int32 | int16 | int8 | int
}

func writeBinInt[T binTypesInt](name string, v T, w io.Writer) (int, error) {
	value := []byte(strconv.FormatInt(int64(v), 10))
	return writeBytes(w, binIntTypePrefix, escapeASB(name), space, value)
}

func writeBinFloat(name string, v float64, w io.Writer) (int, error) {
	return writeBytes(w, binFloatTypePrefix, escapeASB(name), space, []byte(strconv.FormatFloat(v, 'g', -1, 64)))
}

func writeBinString(name, v string, w io.Writer) (int, error) {
	return writeBytes(w, binStringTypePrefix, escapeASB(name), space, []byte(strconv.Itoa(len(v))), space, []byte(v))
}

func writeBinBytes(name string, compact bool, v []byte, w io.Writer) (int, error) {
	var (
		prefix  []byte
		encoded []byte
		result  int
		err     error
	)

	switch compact {
	case true:
		prefix = binBytesTypeCompactPrefix
		result, err = writeBytes(w, prefix, escapeASB(name), space, []byte(strconv.Itoa(len(v))), space, v)
	case false:
		prefix = binBytesTypePrefix
		encoded = base64Encode(v)
		result, err = writeBytes(w, prefix, escapeASB(name), space, []byte(strconv.Itoa(len(encoded))), space, encoded)
		returnBase64Buffer(encoded)
	}

	return result, err
}

func writeBinHLL(name string, compact bool, v a.HLLValue, w io.Writer) (int, error) {
	var (
		prefix  []byte
		encoded []byte
		result  int
		err     error
	)

	switch compact {
	case true:
		prefix = binHLLTypeCompactPrefix
		result, err = writeBytes(w, prefix, escapeASB(name), space, []byte(strconv.Itoa(len(v))), space, v)
	case false:
		prefix = binHLLTypePrefix
		encoded = base64Encode(v)
		result, err = writeBytes(w, prefix, escapeASB(name), space, []byte(strconv.Itoa(len(encoded))), space, encoded)
		returnBase64Buffer(encoded)
	}

	return result, err
}

func writeBinGeoJSON(name string, v a.GeoJSONValue, w io.Writer) (int, error) {
	return writeBytes(w, binGeoJSONTypePrefix, escapeASB(name), space, []byte(strconv.Itoa(len(v))), space, []byte(v))
}

func writeBinNil(name string, w io.Writer) (int, error) {
	return writeBytes(w, binNilTypePrefix, escapeASB(name))
}

func writeRawBlobBin(cdt *a.RawBlobValue, name string, compact bool, w io.Writer) (int, error) {
	switch cdt.ParticleType {
	case particleType.MAP:
		return writeRawMapBin(cdt, name, compact, w)
	case particleType.LIST:
		return writeRawListBin(cdt, name, compact, w)
	default:
		return 0, fmt.Errorf("invalid raw blob bin particle type: %v", cdt.ParticleType)
	}
}

func writeRawMapBin(cdt *a.RawBlobValue, name string, compact bool, w io.Writer) (int, error) {
	var (
		prefix  []byte
		v       []byte
		encoded []byte
		result  int
		err     error
	)

	switch compact {
	case true:
		prefix = binMapTypeCompactPrefix
		v = cdt.Data
		result, err = writeBytes(w, prefix, escapeASB(name), space, []byte(strconv.Itoa(len(v))), space, v)
	case false:
		prefix = binMapTypePrefix
		encoded = base64Encode(cdt.Data)
		result, err = writeBytes(w, prefix, escapeASB(name), space, []byte(strconv.Itoa(len(encoded))), space, encoded)
		returnBase64Buffer(encoded)
	}

	return result, err
}

func writeRawListBin(cdt *a.RawBlobValue, name string, compact bool, w io.Writer) (int, error) {
	var (
		prefix  []byte
		v       []byte
		encoded []byte
		result  int
		err     error
	)

	switch compact {
	case true:
		prefix = binListTypeCompactPrefix
		v = cdt.Data
		result, err = writeBytes(w, prefix, escapeASB(name), space, []byte(strconv.Itoa(len(v))), space, v)
	case false:
		prefix = binListTypePrefix
		encoded = base64Encode(cdt.Data)
		result, err = writeBytes(w, prefix, escapeASB(name), space, []byte(strconv.Itoa(len(encoded))), space, encoded)
		returnBase64Buffer(encoded)
	}

	return result, err
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

func keyToASB(k *a.Key, w io.Writer) (int, error) {
	var bytesWritten int

	userKey := k.Value()
	if userKey != nil {
		n, err := userKeyToASB(k.Value(), w)
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
	New: func() interface{} {
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

func writeBytes(w io.Writer, data ...[]byte) (int, error) {
	totalBytesWritten := 0

	for _, d := range data {
		n, err := w.Write(d)
		if err != nil {
			return totalBytesWritten, err
		}

		totalBytesWritten += n
	}

	n, err := w.Write(newLine)
	if err != nil {
		return totalBytesWritten, err
	}

	totalBytesWritten += n

	return totalBytesWritten, nil
}

func writeRecordNamespace(namespace string, w io.Writer) (int, error) {
	return writeBytes(w, namespacePrefix, escapeASB(namespace))
}

func writeRecordDigest(digest []byte, w io.Writer) (int, error) {
	encoded := base64Encode(digest)
	n, err := writeBytes(w, digestPrefix, encoded)
	returnBase64Buffer(encoded)

	return n, err
}

func writeRecordSet(setName string, w io.Writer) (int, error) {
	return writeBytes(w, setPrefix, escapeASB(setName))
}

func userKeyToASB(userKey a.Value, w io.Writer) (int, error) {
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

func writeUserKeyInt[T UserKeyTypesInt](v T, w io.Writer) (int, error) {
	return writeBytes(
		w,
		recordHeader,
		space,
		recordHeaderType,
		space,
		headerTypeInt,
		space,
		[]byte(strconv.FormatInt(int64(v), 10)),
	)
}

func writeUserKeyFloat(v float64, w io.Writer) (int, error) {
	return writeBytes(
		w,
		recordHeader,
		space,
		recordHeaderType,
		space,
		headerTypeFloat,
		space,
		[]byte(strconv.FormatFloat(v, 'f', -1, 64)),
	)
}

func writeUserKeyString(v string, w io.Writer) (int, error) {
	return writeBytes(
		w,
		recordHeader,
		space,
		recordHeaderType,
		space,
		headerTypeString,
		space,
		[]byte(strconv.Itoa(len(v))),
		space,
		[]byte(v),
	)
}

func writeUserKeyBytes(v []byte, w io.Writer) (int, error) {
	encoded := base64Encode(v)
	n, err := writeBytes(w,
		recordHeader,
		space,
		recordHeaderType,
		space,
		headerTypeBytes,
		space,
		[]byte(strconv.Itoa(len(encoded))),
		space,
		encoded,
	)

	returnBase64Buffer(encoded)

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

// **** SINDEX ****
func sindexToASB(sindex *models.SIndex, w io.Writer) (int, error) {
	// sindexes only ever use 1 path for now
	numPaths := 1

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
		[]byte(strconv.Itoa(numPaths)),
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

func udfToASB(udf *models.UDF, w io.Writer) (int, error) {
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
		[]byte(strconv.Itoa(len(udf.Content))),
		space,
		udf.Content,
	)
}
