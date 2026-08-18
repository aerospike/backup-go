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

const maxPrecomputedHeaderUint = 1000

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

	// cacheLine enables recentLine caching for namespace/set metadata lines.
	// cacheGen enables precomputed generation and bin-count header lines (<= 1000).
	// Both default to true; see BenchmarkEncoderCache in encode_test.go.
	cacheLine bool
	cacheGen  bool
}

// NewEncoder creates a new Encoder.
func NewEncoder[T models.TokenConstraint](cfg *EncoderConfig) *Encoder[T] {
	return &Encoder[T]{
		config:    cfg,
		cacheLine: true,
		cacheGen:  true,
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
		dst = appendUDFToASB(dst, t.UDF)
	case models.TokenTypeSIndex:
		dst = appendSIndexToASB(dst, t.SIndex)
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
	dst := make([]byte, 0, 1024)
	dst = appendVersionText(dst, e.headerVersion(isRecords))
	dst = appendNamespaceMetaText(dst, e.config.Namespace)

	if !e.firstFileWritten.Swap(true) {
		dst = appendFirstMetaText(dst)
	}

	return dst
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
	dst = appendGenerationLine(dst, r.Generation, e.cacheGen)
	dst = append(dst, headerExpiration...)
	dst = append(dst, strconv.AppendInt(number[:0], r.VoidTime, 10)...)
	dst = append(dst, '\n')
	dst = appendBinCountLine(dst, uint32(len(r.Bins)), e.cacheGen)

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

	dst = e.recordNamespace.appendLine(dst, namespacePrefix, key.Namespace(), e.cacheLine)

	dst = append(dst, digestPrefix...)
	dst = appendBase64(dst, key.Digest())
	dst = append(dst, '\n')

	if set := key.SetName(); set != "" {
		dst = e.recordSet.appendLine(dst, setPrefix, set, e.cacheLine)
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

	dst = append(dst, userKeyIntPrefix...)
	dst = append(dst, strconv.AppendInt(number[:0], value, 10)...)

	return append(dst, '\n')
}

func appendUserKeyFloat(dst []byte, value float64) []byte {
	var number [32]byte

	dst = append(dst, userKeyFloatPrefix...)
	dst = append(dst, strconv.AppendFloat(number[:0], value, 'f', -1, 64)...)

	return append(dst, '\n')
}

func appendUserKeyString(dst []byte, value string) []byte {
	var number [20]byte

	dst = append(dst, userKeyStringPrefix...)
	dst = append(dst, strconv.AppendInt(number[:0], int64(len(value)), 10)...)
	dst = append(dst, ' ')
	dst = append(dst, value...)

	return append(dst, '\n')
}

func appendUserKeyBytes(dst, value []byte) []byte {
	var number [20]byte

	dst = append(dst, userKeyBytesPrefix...)
	dst = append(dst, strconv.AppendInt(number[:0], int64(base64.StdEncoding.EncodedLen(len(value))), 10)...)
	dst = append(dst, ' ')
	dst = appendBase64(dst, value)

	return append(dst, '\n')
}

func (e *Encoder[T]) appendRecordBin(dst []byte, name string, value any, number *[32]byte) ([]byte, error) {
	switch value := value.(type) {
	case bool:
		dst = appendBinName(dst, binBoolTypePrefix, name, ' ')
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
		dst = appendBinName(dst, binFloatTypePrefix, name, ' ')
		dst = append(dst, strconv.AppendFloat(number[:0], value, 'g', -1, 64)...)

		return append(dst, '\n'), nil
	case string:
		return appendStringBin(dst, binStringTypePrefix, name, value, number), nil
	case []byte:
		if e.config.Compact {
			return appendRawBin(dst, binBytesTypeCompactPrefix, name, value, number), nil
		}

		return appendBase64Bin(dst, binBytesTypePrefix, name, value, number), nil
	case a.HLLValue:
		if e.config.Compact {
			return appendRawBin(dst, binHLLTypeCompactPrefix, name, value, number), nil
		}

		return appendBase64Bin(dst, binHLLTypePrefix, name, value, number), nil
	case a.GeoJSONValue:
		return appendStringBin(dst, binGeoJSONTypePrefix, name, string(value), number), nil
	case *a.RawBlobValue:
		return e.appendRawBlobBin(dst, name, value, number)
	case nil:
		return appendBinName(dst, binNilTypePrefix, name, '\n'), nil
	default:
		return dst, fmt.Errorf("unknown bin type: %T, key: %s", value, name)
	}
}

func appendBinInt(dst []byte, name string, value int64, number *[32]byte) []byte {
	dst = appendBinName(dst, binIntTypePrefix, name, ' ')
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
			return appendRawBin(dst, binMapTypeCompactPrefix, name, value.Data, number), nil
		}

		return appendBase64Bin(dst, binMapTypePrefix, name, value.Data, number), nil
	case particleType.LIST:
		if e.config.Compact {
			return appendRawBin(dst, binListTypeCompactPrefix, name, value.Data, number), nil
		}

		return appendBase64Bin(dst, binListTypePrefix, name, value.Data, number), nil
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

func appendGenerationLine(dst []byte, generation uint32, cache bool) []byte {
	if cache && generation <= maxPrecomputedHeaderUint {
		return append(dst, generationLines[generation]...)
	}

	var number [32]byte

	dst = append(dst, headerGeneration...)
	dst = append(dst, strconv.AppendUint(number[:0], uint64(generation), 10)...)

	return append(dst, '\n')
}

func appendBinCountLine(dst []byte, binCount uint32, cache bool) []byte {
	if cache && binCount <= maxPrecomputedHeaderUint {
		return append(dst, binCountLines[binCount]...)
	}

	var number [32]byte

	dst = append(dst, headerBinCount...)
	dst = append(dst, strconv.AppendUint(number[:0], uint64(binCount), 10)...)

	return append(dst, '\n')
}

type recentLine struct {
	value string
	line  []byte
}

// recentLine caches the most recently used metadata line for sequential records.
func (c *recentLine) appendLine(dst, prefix []byte, value string, cache bool) []byte {
	if !cache {
		return appendMetadataLine(dst, prefix, value)
	}

	if c.value == value {
		return append(dst, c.line...)
	}

	c.value = value
	c.line = appendMetadataLine(c.line[:0], prefix, value)

	return append(dst, c.line...)
}

func appendMetadataLine(dst, prefix []byte, value string) []byte {
	dst = append(dst, prefix...)
	dst = appendEscapedDirect(dst, value)

	return append(dst, '\n')
}

// **** META DATA ****

func appendVersionText(dst []byte, asbVersion string) []byte {
	dst = append(dst, tokenVersion...)
	dst = append(dst, space...)
	dst = append(dst, asbVersion...)

	return append(dst, '\n')
}

func appendNamespaceMetaText(dst []byte, namespace string) []byte {
	dst = append(dst, metadataSection...)
	dst = append(dst, space...)
	dst = append(dst, namespaceToken...)
	dst = append(dst, space...)
	dst = appendEscapedDirect(dst, namespace)

	return append(dst, '\n')
}

func appendFirstMetaText(dst []byte) []byte {
	dst = append(dst, metadataSection...)
	dst = append(dst, space...)
	dst = append(dst, tokenFirst...)

	return append(dst, '\n')
}

var needsEscape = [256]bool{
	'\\': true,
	' ':  true,
	'\n': true,
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

func appendSIndexToASB(dst []byte, sindex *models.SIndex) []byte {
	dst = append(dst, globalSection...)
	dst = append(dst, space...)

	if sindex.Expression != "" {
		dst = append(dst, globalSIndexExpression...)
	} else {
		dst = append(dst, globalSIndex...)
	}

	dst = append(dst, space...)
	dst = appendEscapedDirect(dst, sindex.Namespace)
	dst = append(dst, space...)
	dst = appendEscapedDirect(dst, sindex.Set)
	dst = append(dst, space...)
	dst = appendEscapedDirect(dst, sindex.Name)
	dst = append(dst, space...)
	dst = append(dst, byte(sindex.IndexType))
	dst = append(dst, space...)
	dst = append(dst, sindexSizeOne...)
	dst = append(dst, space...)
	dst = appendEscapedDirect(dst, sindex.Path.BinName)
	dst = append(dst, space...)
	dst = append(dst, byte(sindex.Path.BinType))

	if sindex.Path.B64Context != "" {
		dst = append(dst, space...)
		dst = append(dst, sindex.Path.B64Context...)
	}

	if sindex.Expression != "" {
		dst = append(dst, space...)
		dst = append(dst, sindex.Expression...)
	}

	return append(dst, '\n')
}

// **** UDFs ****

func appendUDFToASB(dst []byte, udf *models.UDF) []byte {
	var lenBuf [20]byte
	contentLen := strconv.AppendInt(lenBuf[:0], int64(len(udf.Content)), 10)

	dst = append(dst, globalSection...)
	dst = append(dst, space...)
	dst = append(dst, globalUDF...)
	dst = append(dst, space...)
	dst = append(dst, byte(udf.UDFType))
	dst = append(dst, space...)
	dst = appendEscapedDirect(dst, udf.Name)
	dst = append(dst, space...)
	dst = append(dst, contentLen...)
	dst = append(dst, space...)
	dst = append(dst, udf.Content...)

	return append(dst, '\n')
}
