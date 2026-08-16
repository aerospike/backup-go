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
	"slices"
	"strconv"
	"sync/atomic"

	a "github.com/aerospike/aerospike-client-go/v8"
	particleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
	"github.com/aerospike/backup-go/models"
	"github.com/segmentio/asm/base64"
)

const maxPrecomputedHeaderUint = 100

var (
	generationLines [maxPrecomputedHeaderUint + 1][]byte
	binCountLines   [maxPrecomputedHeaderUint + 1][]byte
)

func init() {
	for i := range generationLines {
		generationLines[i] = fmt.Appendf(nil, "+ g %d\n", i)
		binCountLines[i] = fmt.Appendf(nil, "+ b %d\n", i)
	}
}

// Encoder contains logic for encoding backup data into the .asb format.
// This is a stateful object that must be created for every backup operation.
type Encoder[T models.TokenConstraint] struct {
	config           *EncoderConfig
	recordNamespace  recentLine
	recordSet        recentLine
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

// EncodeToken appends the encoded token to dst and returns the extended slice.
func (e *Encoder[T]) EncodeToken(token T, dst []byte) ([]byte, error) {
	t, ok := any(token).(*models.Token)
	if !ok {
		return dst, fmt.Errorf("unsupported token type %T for ASB encoder", token)
	}

	start := len(dst)

	var err error

	switch t.Type {
	case models.TokenTypeRecord:
		dst, err = e.appendRecord(dst, t.Record)
	case models.TokenTypeUDF:
		dst, err = appendUDFToken(dst, t.UDF)
	case models.TokenTypeSIndex:
		dst, err = appendSIndexToken(dst, t.SIndex)
	case models.TokenTypeInvalid:
		err = errors.New("invalid token")
	default:
		err = fmt.Errorf("invalid token type: %v", t.Type)
	}

	if err != nil {
		return dst, fmt.Errorf("failed to encode token at byte %d: %w", len(dst)-start, err)
	}

	return dst, nil
}

// GetHeader returns the header of the ASB file as a byte slice.
// The header contains the version, namespace, and first file flag.
func (e *Encoder[T]) GetHeader(isRecords bool) []byte {
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

func (e *Encoder[T]) appendRecord(dst []byte, r *models.Record) ([]byte, error) {
	dst, err := e.appendRecordKey(dst, r.Key)
	if err != nil {
		return dst, err
	}

	var number [32]byte
	dst = appendGenerationLine(dst, r.Generation)
	dst = append(dst, "+ t "...)
	dst = append(dst, strconv.AppendInt(number[:0], r.VoidTime, 10)...)
	dst = append(dst, '\n')
	dst = appendBinCountLine(dst, uint32(len(r.Bins)))

	for name, value := range r.Bins {
		dst, err = e.appendRecordBin(dst, name, value, &number)
		if err != nil {
			return dst, err
		}
	}

	return dst, nil
}

func (e *Encoder[T]) appendRecordKey(dst []byte, key *a.Key) ([]byte, error) {
	if userKey := key.Value(); userKey != nil {
		var err error

		dst, err = appendUserKey(dst, userKey)
		if err != nil {
			return dst, err
		}
	}

	dst = e.recordNamespace.appendLine(dst, []byte("+ n "), key.Namespace())

	dst = append(dst, "+ d "...)
	dst = appendBase64(dst, key.Digest())
	dst = append(dst, '\n')

	if set := key.SetName(); set != "" {
		dst = e.recordSet.appendLine(dst, []byte("+ s "), set)
	}

	return dst, nil
}

func appendUserKey(dst []byte, userKey a.Value) ([]byte, error) {
	switch value := userKey.(type) {
	case a.IntegerValue:
		return appendUserKeyInt(dst, int64(value)), nil
	case a.LongValue:
		return appendUserKeyInt(dst, int64(value)), nil
	case a.FloatValue:
		return appendUserKeyFloat(dst, float64(value)), nil
	case a.StringValue:
		return appendUserKeyString(dst, string(value)), nil
	case a.BytesValue:
		return appendUserKeyBytes(dst, value), nil
	case a.NullValue:
		return dst, nil
	}

	switch value := userKey.GetObject().(type) {
	case int64:
		return appendUserKeyInt(dst, value), nil
	case int32:
		return appendUserKeyInt(dst, int64(value)), nil
	case int16:
		return appendUserKeyInt(dst, int64(value)), nil
	case int8:
		return appendUserKeyInt(dst, int64(value)), nil
	case int:
		return appendUserKeyInt(dst, int64(value)), nil
	case float64:
		return appendUserKeyFloat(dst, value), nil
	case string:
		return appendUserKeyString(dst, value), nil
	case []byte:
		return appendUserKeyBytes(dst, value), nil
	case nil:
		return dst, nil
	default:
		return dst, fmt.Errorf("invalid user key type: %T", value)
	}
}

func appendUserKeyInt(dst []byte, value int64) []byte {
	var number [20]byte

	dst = append(dst, "+ k I "...)
	dst = append(dst, strconv.AppendInt(number[:0], value, 10)...)

	return append(dst, '\n')
}

func appendUserKeyFloat(dst []byte, value float64) []byte {
	var number [32]byte

	dst = append(dst, "+ k D "...)
	dst = append(dst, strconv.AppendFloat(number[:0], value, 'f', -1, 64)...)

	return append(dst, '\n')
}

func appendUserKeyString(dst []byte, value string) []byte {
	var number [20]byte

	dst = append(dst, "+ k S "...)
	dst = append(dst, strconv.AppendInt(number[:0], int64(len(value)), 10)...)
	dst = append(dst, ' ')
	dst = append(dst, value...)

	return append(dst, '\n')
}

func appendUserKeyBytes(dst, value []byte) []byte {
	var number [20]byte

	dst = append(dst, "+ k B "...)
	dst = append(dst, strconv.AppendInt(number[:0], int64(base64.StdEncoding.EncodedLen(len(value))), 10)...)
	dst = append(dst, ' ')
	dst = appendBase64(dst, value)

	return append(dst, '\n')
}

func (e *Encoder[T]) appendRecordBin(dst []byte, name string, value any, number *[32]byte) ([]byte, error) {
	switch value := value.(type) {
	case bool:
		dst = appendBinName(dst, []byte("- Z "), name, ' ')
		if value {
			dst = append(dst, 'T', '\n')
		} else {
			dst = append(dst, 'F', '\n')
		}

		return dst, nil
	case int64:
		return appendBinInt(dst, name, value, number), nil
	case int32:
		return appendBinInt(dst, name, int64(value), number), nil
	case int16:
		return appendBinInt(dst, name, int64(value), number), nil
	case int8:
		return appendBinInt(dst, name, int64(value), number), nil
	case int:
		return appendBinInt(dst, name, int64(value), number), nil
	case float64:
		dst = appendBinName(dst, []byte("- D "), name, ' ')
		dst = append(dst, strconv.AppendFloat(number[:0], value, 'g', -1, 64)...)

		return append(dst, '\n'), nil
	case string:
		return appendStringBin(dst, []byte("- S "), name, value, number), nil
	case []byte:
		if e.config.Compact {
			return appendRawBin(dst, []byte("- B! "), name, value, number), nil
		}

		return appendBase64Bin(dst, []byte("- B "), name, value, number), nil
	case a.HLLValue:
		if e.config.Compact {
			return appendRawBin(dst, []byte("- Y! "), name, value, number), nil
		}

		return appendBase64Bin(dst, []byte("- Y "), name, value, number), nil
	case a.GeoJSONValue:
		return appendStringBin(dst, []byte("- G "), name, string(value), number), nil
	case *a.RawBlobValue:
		return e.appendRawBlobBin(dst, name, value, number)
	case nil:
		return appendBinName(dst, []byte("- N "), name, '\n'), nil
	default:
		return dst, fmt.Errorf("unknown bin type: %T, key: %s", value, name)
	}
}

func appendBinInt(dst []byte, name string, value int64, number *[32]byte) []byte {
	dst = appendBinName(dst, []byte("- I "), name, ' ')
	dst = append(dst, strconv.AppendInt(number[:0], value, 10)...)

	return append(dst, '\n')
}

func appendStringBin(dst, prefix []byte, name, value string, number *[32]byte) []byte {
	dst = appendBinName(dst, prefix, name, ' ')
	dst = append(dst, strconv.AppendInt(number[:0], int64(len(value)), 10)...)
	dst = append(dst, ' ')
	dst = append(dst, value...)

	return append(dst, '\n')
}

func appendRawBin(dst, prefix []byte, name string, value []byte, number *[32]byte) []byte {
	dst = appendBinName(dst, prefix, name, ' ')
	dst = append(dst, strconv.AppendInt(number[:0], int64(len(value)), 10)...)
	dst = append(dst, ' ')
	dst = append(dst, value...)

	return append(dst, '\n')
}

func appendBase64Bin(dst, prefix []byte, name string, value []byte, number *[32]byte) []byte {
	dst = appendBinName(dst, prefix, name, ' ')
	dst = append(dst, strconv.AppendInt(number[:0], int64(base64.StdEncoding.EncodedLen(len(value))), 10)...)
	dst = append(dst, ' ')
	dst = appendBase64(dst, value)

	return append(dst, '\n')
}

func (e *Encoder[T]) appendRawBlobBin(
	dst []byte, name string, value *a.RawBlobValue, number *[32]byte,
) ([]byte, error) {
	switch value.ParticleType {
	case particleType.MAP:
		if e.config.Compact {
			return appendRawBin(dst, []byte("- M! "), name, value.Data, number), nil
		}

		return appendBase64Bin(dst, []byte("- M "), name, value.Data, number), nil
	case particleType.LIST:
		if e.config.Compact {
			return appendRawBin(dst, []byte("- L! "), name, value.Data, number), nil
		}

		return appendBase64Bin(dst, []byte("- L "), name, value.Data, number), nil
	default:
		return dst, fmt.Errorf("invalid raw blob bin particle type: %v", value.ParticleType)
	}
}

func appendBinName(dst, prefix []byte, name string, suffix byte) []byte {
	dst = append(dst, prefix...)
	dst = appendEscapedDirect(dst, name)

	return append(dst, suffix)
}

func appendBase64(dst, value []byte) []byte {
	encodedLen := base64.StdEncoding.EncodedLen(len(value))
	offset := len(dst)
	dst = slices.Grow(dst, encodedLen)
	dst = dst[:offset+encodedLen]
	base64.StdEncoding.Encode(dst[offset:], value)

	return dst
}

func appendGenerationLine(dst []byte, generation uint32) []byte {
	if generation <= maxPrecomputedHeaderUint {
		return append(dst, generationLines[generation]...)
	}

	var number [32]byte

	dst = append(dst, "+ g "...)
	dst = append(dst, strconv.AppendUint(number[:0], uint64(generation), 10)...)

	return append(dst, '\n')
}

func appendBinCountLine(dst []byte, binCount uint32) []byte {
	if binCount <= maxPrecomputedHeaderUint {
		return append(dst, binCountLines[binCount]...)
	}

	var number [32]byte

	dst = append(dst, "+ b "...)
	dst = append(dst, strconv.AppendUint(number[:0], uint64(binCount), 10)...)

	return append(dst, '\n')
}

type cachedLine struct {
	value string
	line  []byte
}

// recentLine caches the most recently used metadata line. The hot path uses
// only an atomic load and string comparison, so it is effective for sequential
// records while remaining correct when namespaces or sets change.
type recentLine struct {
	entry atomic.Pointer[cachedLine]
}

func (c *recentLine) appendLine(dst, prefix []byte, value string) []byte {
	if entry := c.entry.Load(); entry != nil && entry.value == value {
		return append(dst, entry.line...)
	}

	line := appendMetadataLine(nil, prefix, value)
	c.entry.Store(&cachedLine{value: value, line: line})

	return append(dst, line...)
}

func appendMetadataLine(dst, prefix []byte, value string) []byte {
	dst = append(dst, prefix...)
	dst = appendEscapedDirect(dst, value)

	return append(dst, '\n')
}

func appendUDFToken(dst []byte, udf *models.UDF) ([]byte, error) {
	w := bytes.NewBuffer(dst)
	_, err := udfToASB(udf, w)

	return w.Bytes(), err
}

func appendSIndexToken(dst []byte, sindex *models.SIndex) ([]byte, error) {
	w := bytes.NewBuffer(dst)
	_, err := sindexToASB(sindex, w)

	return w.Bytes(), err
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

var needsEscape = [256]bool{
	'\\': true,
	' ':  true,
	'\n': true,
}

func escapeASB(s string) []byte {
	return appendEscapedDirect(nil, s)
}

func appendEscapedDirect(dst []byte, value string) []byte {
	start := 0

	for i := 0; i < len(value); i++ {
		if !needsEscape[value[i]] {
			continue
		}

		dst = append(dst, value[start:i]...)
		dst = append(dst, '\\', value[i])
		start = i + 1
	}

	return append(dst, value[start:]...)
}

// **** SINDEX ****

func sindexToASB(sindex *models.SIndex, w *bytes.Buffer) (int, error) {
	sindexSection := globalSIndex
	if sindex.Expression != "" {
		sindexSection = globalSIndexExpression
	}

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

	if sindex.Path.B64Context != "" {
		params = append(params, space, []byte(sindex.Path.B64Context))
	}

	if sindex.Expression != "" {
		params = append(params, space, []byte(sindex.Expression))
	}

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
