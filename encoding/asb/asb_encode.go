// Copyright 2024-2024 Aerospike, Inc.
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
	"encoding/base64"
	"errors"
	"fmt"
	"io"

	"github.com/aerospike/backup-go/models"

	a "github.com/aerospike/aerospike-client-go/v7"
	particleType "github.com/aerospike/aerospike-client-go/v7/types/particle_type"
)

type Encoder struct {
	buff bytes.Buffer
}

func NewEncoder() (*Encoder, error) {
	return &Encoder{
		buff: bytes.Buffer{},
	}, nil
}

// EncodeToken encodes a token to the ASB format.
// It returns a byte slice of the encoded token
// and an error if the encoding fails.
// The returned byte slice is only valid until the next call to EncodeToken.
func (o *Encoder) EncodeToken(token *models.Token) ([]byte, error) {
	var (
		n   int
		err error
	)

	o.buff.Reset()

	switch token.Type {
	case models.TokenTypeRecord:
		n, err = o.encodeRecord(&token.Record)
	case models.TokenTypeUDF:
		data, UDFErr := o.encodeUDF(token.UDF)
		n, err = len(data), UDFErr
	case models.TokenTypeSIndex:
		n, err = o.encodeSIndex(token.SIndex)
	case models.TokenTypeInvalid:
		n, err = 0, errors.New("invalid token")
	default:
		n, err = 0, fmt.Errorf("invalid token type: %v", token.Type)
	}

	if err != nil {
		return nil, fmt.Errorf("error encoding token at byte %d: %w", n, err)
	}

	return o.buff.Bytes(), nil
}

func (o *Encoder) encodeRecord(rec *models.Record) (int, error) {
	return recordToASB(rec, &o.buff)
}

//nolint:unparam // UDF is not implemented yet, return value is nil for now.
func (o *Encoder) encodeUDF(_ *models.UDF) ([]byte, error) {
	return nil, fmt.Errorf("%w: unimplemented", errors.ErrUnsupported)
}

func (o *Encoder) encodeSIndex(sindex *models.SIndex) (int, error) {
	return sindexToASB(sindex, &o.buff)
}

func GetHeader(namespace string, firstFile bool) ([]byte, error) {
	// capacity is arbitrary, just probably enough to avoid reallocations
	data := make([]byte, 0, 256)
	buff := bytes.NewBuffer(data)

	_, err := writeVersionText(ASBFormatVersion, buff)
	if err != nil {
		return nil, err
	}

	_, err = writeNamespaceMetaText(namespace, buff)
	if err != nil {
		return nil, err
	}

	if firstFile {
		_, err = writeFirstMetaText(buff)
		if err != nil {
			return nil, err
		}
	}

	return buff.Bytes(), nil
}

// **** META DATA ****

func writeVersionText(asbVersion string, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "Version %s\n", asbVersion)
}

func writeNamespaceMetaText(namespace string, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c namespace %s\n", markerMetadataSection, escapeASB(namespace))
}

func writeFirstMetaText(w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c first-file\n", markerMetadataSection)
}

// **** RECORD ****

func recordToASB(r *models.Record, w io.Writer) (int, error) {
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

	n, err = binsToASB(r.Bins, w)
	bytesWritten += n

	if err != nil {
		return bytesWritten, err
	}

	return bytesWritten, nil
}

func writeRecordHeaderGeneration(generation uint32, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %d\n", markerRecordHeader, recordHeaderTypeGen, generation)
}

func writeRecordHeaderExpiration(expiration int64, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %d\n", markerRecordHeader, recordHeaderTypeExpiration, expiration)
}

func writeRecordHeaderBinCount(binCount int, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %d\n", markerRecordHeader, recordHeaderTypeBinCount, binCount)
}

func binsToASB(bins a.BinMap, w io.Writer) (int, error) {
	var bytesWritten int

	// NOTE golang's random order map iteration
	// means that any backup files that include
	// multi element bin maps may not be identical
	// over multiple backups even if the data is the same
	for k, v := range bins {
		n, err := binToASB(k, v, w)
		bytesWritten += n

		if err != nil {
			return bytesWritten, err
		}
	}

	return bytesWritten, nil
}

func binToASB(k string, v any, w io.Writer) (int, error) {
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
		bytesWritten, err = writeBinBytes(k, v, w)
	case *a.RawBlobValue:
		bytesWritten, err = writeRawBlobBin(v, k, w)
	case a.HLLValue:
		bytesWritten, err = writeBinHLL(k, v, w)
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
	return fmt.Fprintf(w, "%c %c %s %c\n", markerRecordBins, binTypeBool, escapeASB(name), boolToASB(v))
}

type binTypesInt interface {
	int64 | int32 | int16 | int8 | int
}

func writeBinInt[T binTypesInt](name string, v T, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %s %d\n", markerRecordBins, binTypeInt, escapeASB(name), v)
}

func writeBinFloat(name string, v float64, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %s %f\n", markerRecordBins, binTypeFloat, escapeASB(name), v)
}

func writeBinString(name, v string, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %s %d %s\n", markerRecordBins, binTypeString, escapeASB(name), len(v), v)
}

func writeBinBytes(name string, v []byte, w io.Writer) (int, error) {
	encoded := base64Encode(v)
	return fmt.Fprintf(w, "%c %c %s %d %s\n", markerRecordBins, binTypeBytes, escapeASB(name), len(encoded), encoded)
}

func writeBinHLL(name string, v a.HLLValue, w io.Writer) (int, error) {
	encoded := base64Encode(v)
	return fmt.Fprintf(w, "%c %c %s %d %s\n", markerRecordBins, binTypeBytesHLL, escapeASB(name), len(encoded), encoded)
}

func writeBinGeoJSON(name string, v a.GeoJSONValue, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %s %d %s\n", markerRecordBins, binTypeGeoJSON, escapeASB(name), len(v), v)
}

func writeBinNil(name string, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %s\n", markerRecordBins, binTypeNil, escapeASB(name))
}

func writeRawBlobBin(cdt *a.RawBlobValue, name string, w io.Writer) (int, error) {
	switch cdt.ParticleType {
	case particleType.MAP:
		return writeRawMapBin(cdt, name, w)
	case particleType.LIST:
		return writeRawListBin(cdt, name, w)
	default:
		return 0, fmt.Errorf("invalid raw blob bin particle type: %v", cdt.ParticleType)
	}
}

func writeRawMapBin(cdt *a.RawBlobValue, name string, w io.Writer) (int, error) {
	encoded := base64Encode(cdt.Data)
	return fmt.Fprintf(w, "%c %c %s %d %s\n", markerRecordBins, binTypeBytesMap, escapeASB(name), len(encoded), encoded)
}

func writeRawListBin(cdt *a.RawBlobValue, name string, w io.Writer) (int, error) {
	encoded := base64Encode(cdt.Data)
	return fmt.Fprintf(w, "%c %c %s %d %s\n", markerRecordBins, binTypeBytesList, escapeASB(name), len(encoded), encoded)
}

func blobBinToASB(val []byte, bytesType byte, name string) []byte {
	return []byte(fmt.Sprintf("%c %s %d %s\n", bytesType, name, len(val), val))
}

func boolToASB(b bool) byte {
	if b {
		return boolTrueByte
	}

	return boolFalseByte
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

func base64Encode(v []byte) []byte {
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(v)))
	base64.StdEncoding.Encode(encoded, v)

	return encoded
}

func writeRecordNamespace(namespace string, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %s\n", markerRecordHeader, recordHeaderTypeNamespace, escapeASB(namespace))
}

func writeRecordDigest(digest []byte, w io.Writer) (int, error) {
	encoded := base64Encode(digest)
	return fmt.Fprintf(w, "%c %c %s\n", markerRecordHeader, recordHeaderTypeDigest, encoded)
}

func writeRecordSet(setName string, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %s\n", markerRecordHeader, recordHeaderTypeSet, escapeASB(setName))
}

func userKeyToASB(userKey a.Value, w io.Writer) (int, error) {
	val := userKey.GetObject()
	switch v := val.(type) {
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
	default:
		return 0, fmt.Errorf("invalid user key type: %T", v)
	}
}

type UserKeyTypesInt interface {
	int64 | int32 | int16 | int8 | int
}

func writeUserKeyInt[T UserKeyTypesInt](v T, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %c %d\n", markerRecordHeader, recordHeaderTypeKey, keyTypeInt, v)
}

func writeUserKeyFloat(v float64, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %c %f\n", markerRecordHeader, recordHeaderTypeKey, keyTypeFloat, v)
}

func writeUserKeyString(v string, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %c %d %s\n", markerRecordHeader, recordHeaderTypeKey, keyTypeString, len(v), v)
}

func writeUserKeyBytes(v []byte, w io.Writer) (int, error) {
	encoded := base64Encode(v)
	return fmt.Fprintf(w, "%c %c %c %d %s\n", markerRecordHeader, recordHeaderTypeKey, keyTypeBytes, len(encoded), encoded)
}

// **** SINDEX ****

// control characters
var asbEscapedChars = map[byte]struct{}{
	'\\': {},
	' ':  {},
	'\n': {},
}

func escapeASB(s string) string {
	escapeCount := 0

	for _, c := range s {
		if _, ok := asbEscapedChars[byte(c)]; ok {
			escapeCount++
		}
	}

	if escapeCount == 0 {
		return s
	}

	escaped := make([]byte, len(s)+escapeCount)
	i := 0

	for _, c := range s {
		if _, ok := asbEscapedChars[byte(c)]; ok {
			escaped[i] = '\\'
			i++
		}

		escaped[i] = byte(c)
		i++
	}

	return string(escaped)
}

func sindexToASB(sindex *models.SIndex, w io.Writer) (int, error) {
	var bytesWritten int

	// sindexes only ever use 1 path for now
	numPaths := 1

	n, err := fmt.Fprintf(
		w,
		"%c %c %s %s %s %c %d %s %c",
		markerGlobalSection,
		globalTypeSIndex,
		escapeASB(sindex.Namespace),
		escapeASB(sindex.Set),
		escapeASB(sindex.Name),
		byte(sindex.IndexType),
		numPaths,
		escapeASB(sindex.Path.BinName),
		byte(sindex.Path.BinType),
	)
	bytesWritten += n

	if err != nil {
		return bytesWritten, err
	}

	if sindex.Path.B64Context != "" {
		n, err = fmt.Fprintf(w, " %s", sindex.Path.B64Context)
		bytesWritten += n

		if err != nil {
			return bytesWritten, err
		}
	}

	n, err = fmt.Fprintf(w, "\n")
	bytesWritten += n

	return bytesWritten, err
}
