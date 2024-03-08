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
	"encoding/base64"
	"errors"
	"fmt"
	"io"

	"github.com/aerospike/backup-go/models"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type Encoder struct {
	output io.Writer
}

func NewEncoder(output io.Writer) (*Encoder, error) {
	return &Encoder{
		output: output,
	}, nil
}

func (o *Encoder) EncodeToken(token *models.Token) (int, error) {
	switch token.Type {
	case models.TokenTypeRecord:
		return o.EncodeRecord(&token.Record)
	// case models.TokenTypeUDF:
	// 	return o.EncodeUDF(token.UDF)
	// case models.TokenTypeSIndex:
	// 	return o.EncodeSIndex(token.SIndex)
	case models.TokenTypeInvalid:
		return 0, errors.New("invalid token")
	default:
		return 0, fmt.Errorf("invalid token type: %v", token.Type)
	}
}

func (o *Encoder) EncodeRecord(rec *models.Record) (int, error) {
	return recordToASB(rec, o.output)
}

func (o *Encoder) EncodeUDF(_ *models.UDF) ([]byte, error) {
	return nil, fmt.Errorf("%w: unimplemented", errors.ErrUnsupported)
}

func (o *Encoder) EncodeSIndex(sindex *models.SIndex) ([]byte, error) {
	return sindexToASB(sindex)
}

func (o *Encoder) GetVersionText() []byte {
	return []byte(fmt.Sprintf("Version %s\n", ASBFormatVersion))
}

func (o *Encoder) GetNamespaceMetaText(namespace string) []byte {
	return []byte(fmt.Sprintf("%c namespace %s\n", markerMetadataSection, escapeASBS(namespace)))
}

func (o *Encoder) GetFirstMetaText() []byte {
	return []byte(fmt.Sprintf("%c first-file\n", markerMetadataSection))
}

// **** RECORD ****

func recordToASB(r *models.Record, w io.Writer) (int, error) {
	// bytes written
	var bw int

	// if r == nil {
	// 	return nil, errors.New("record is nil")
	// }

	// if r.Key == nil {
	// 	return bw, errors.New("record key is nil")
	// }

	n, err := keyToASB(r.Key, w)
	bw += n
	if err != nil {
		return bw, err
	}

	n, err = writeRecordHeaderGeneration(r.Generation, w)
	bw += n
	if err != nil {
		return bw, err
	}

	n, err = writeRecordHeaderExpiration(r.VoidTime, w)
	bw += n
	if err != nil {
		return bw, err
	}

	n, err = writeRecordHeaderBinCount(len(r.Bins), w)
	bw += n
	if err != nil {
		return bw, err
	}

	// if len(r.Bins) < 1 {
	// 	return bw, fmt.Errorf("ERR: empty binmap")
	// }

	n, err = binsToASB(r.Bins, w)
	bw += n
	if err != nil {
		return bw, err
	}

	return bw, nil
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
	// bytes written
	var bw int

	if len(bins) < 1 {
		return bw, fmt.Errorf("ERR: empty binmap")
	}

	// NOTE golang's random order map iteration
	// means that any backup files that include
	// multi element bin maps may not be identical
	// over multiple backups even if the data is the same
	for k, v := range bins {
		n, err := binToASB(k, v, w)
		bw += n
		if err != nil {
			return bw, err
		}
	}

	return bw, nil
}

func binToASB(k string, v any, w io.Writer) (int, error) {
	// bytes written
	var (
		bw  int
		err error
	)

	switch v := v.(type) {
	case bool:
		bw, err = writeBoolBin(k, v, w)
	case int64:
		bw, err = writeIntBin(k, v, w)
	case int32:
		bw, err = writeIntBin(k, v, w)
	case int16:
		bw, err = writeIntBin(k, v, w)
	case int8:
		bw, err = writeIntBin(k, v, w)
	case int:
		bw, err = writeIntBin(k, v, w)
	case float64:
		bw, err = writeFloatBin(k, v, w)
	case string:
		bw, err = writeStringBin(k, v, w)
	case []byte:
		bw, err = writeBytesBin(k, v, w)
	case map[any]any:
		return bw, errors.New("map bin not supported")
	case []any:
		return bw, errors.New("list bin not supported")
	case a.HLLValue:
		bw, err = writeHLLBin(k, v, w)
	case a.GeoJSONValue:
		bw, err = writeGeoJSONBin(k, v, w)
	case nil:
		bw, err = writeNilBin(k, w)
	default:
		return bw, fmt.Errorf("unknown bin type: %T, key: %s", v, k)
	}

	return bw, err
}

func writeBoolBin(name string, v bool, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %s %c\n", markerRecordBins, binTypeBool, escapeASBS(name), boolToASB(v))
}

type intBinTypes interface {
	int64 | int32 | int16 | int8 | int
}

func writeIntBin[T intBinTypes](name string, v T, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %s %d\n", markerRecordBins, binTypeInt, escapeASBS(name), v)
}

func writeFloatBin(name string, v float64, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %s %f\n", markerRecordBins, binTypeFloat, escapeASBS(name), v)
}

func writeStringBin(name string, v string, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %s %d %s\n", markerRecordBins, binTypeString, escapeASBS(name), len(v), v)
}

func writeBytesBin(name string, v []byte, w io.Writer) (int, error) {
	encoded := newBase64Encode(v)
	return fmt.Fprintf(w, "%c %c %s %d %s\n", markerRecordBins, binTypeBytes, escapeASBS(name), len(encoded), encoded)
}

func writeHLLBin(name string, v a.HLLValue, w io.Writer) (int, error) {
	encoded := newBase64Encode(v)
	return fmt.Fprintf(w, "%c %c %s %d %s\n", markerRecordBins, binTypeBytesHLL, escapeASBS(name), len(encoded), encoded)
}

func writeGeoJSONBin(name string, v a.GeoJSONValue, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %s %d %s\n", markerRecordBins, binTypeGeoJSON, escapeASBS(name), len(v), v)
}

func writeNilBin(name string, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %s\n", markerRecordBins, binTypeNil, escapeASBS(name))
}

func boolToASB(b bool) byte {
	if b {
		return boolTrueByte
	}

	return boolFalseByte
}

func keyToASB(k *a.Key, w io.Writer) (int, error) {
	// bytes written
	var bw int

	userKey := k.Value()
	if userKey != nil {
		n, err := userKeyToASB(k.Value(), w)
		bw += n
		if err != nil {
			return bw, err
		}
	}

	n, err := writeRecordNamespace(k.Namespace(), w)
	bw += n
	if err != nil {
		return bw, err
	}

	n, err = writeRecordDigest(k.Digest(), w)
	bw += n
	if err != nil {
		return bw, err
	}

	if k.SetName() != "" {
		n, err = writeRecordSet(k.SetName(), w)
		bw += n
		if err != nil {
			return bw, err
		}
	}

	return bw, nil
}

func base64Encode(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func newBase64Encode(v []byte) []byte {
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(v)))
	base64.StdEncoding.Encode(encoded, v)
	return encoded
}

func writeRecordNamespace(namespace string, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %s\n", markerRecordHeader, recordHeaderTypeNamespace, escapeASBS(namespace))
}

func writeRecordDigest(digest []byte, w io.Writer) (int, error) {
	encoded := newBase64Encode(digest)
	return fmt.Fprintf(w, "%c %c %s\n", markerRecordHeader, recordHeaderTypeDigest, encoded)
}

func writeRecordSet(setName string, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %s\n", markerRecordHeader, recordHeaderTypeSet, escapeASBS(setName))
}

func userKeyToASB(userKey a.Value, w io.Writer) (int, error) {
	val := userKey.GetObject()
	switch v := val.(type) {
	// need the repeated int cases to satisfy the generic type checker
	case int64:
		return writeIntUserKey(v, w)
	case int32:
		return writeIntUserKey(v, w)
	case int16:
		return writeIntUserKey(v, w)
	case int8:
		return writeIntUserKey(v, w)
	case int:
		return writeIntUserKey(v, w)
	case float64:
		return writeFloatUserKey(v, w)
	case string:
		return writeStringUserKey(v, w)
	case []byte:
		return writeBytesUserKey(v, w)
	default:
		return 0, fmt.Errorf("invalid user key type: %T", v)
	}
}

type intUserKeyTypes interface {
	int64 | int32 | int16 | int8 | int
}

func writeIntUserKey[T intUserKeyTypes](v T, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %c %d\n", markerRecordHeader, recordHeaderTypeKey, keyTypeInt, v)
}

func writeFloatUserKey(v float64, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %c %f\n", markerRecordHeader, recordHeaderTypeKey, keyTypeFloat, v)
}

func writeStringUserKey(v string, w io.Writer) (int, error) {
	return fmt.Fprintf(w, "%c %c %c %d %s\n", markerRecordHeader, recordHeaderTypeKey, keyTypeString, len(v), v)
}

func writeBytesUserKey(v []byte, w io.Writer) (int, error) {
	encoded := newBase64Encode(v)
	return fmt.Fprintf(w, "%c %c %c %d %s\n", markerRecordHeader, recordHeaderTypeKey, keyTypeBytes, len(encoded), encoded)
}

// **** SINDEX ****

// control characters
var asbEscapedChars = map[byte]struct{}{
	'\\': {},
	' ':  {},
	'\n': {},
}

// TODO improve performance maybe by writing directly to a buffer
func escapeASBS(s string) string {
	in := []byte(s)
	v := []byte{}

	for _, c := range in {
		if _, ok := asbEscapedChars[c]; ok {
			v = append(v, asbEscape)
		}

		v = append(v, c)
	}

	return string(v)
}

func sindexToASB(sindex *models.SIndex) ([]byte, error) {
	if sindex == nil {
		return nil, errors.New("sindex is nil")
	}

	// sindexes only ever use 1 path for now
	numPaths := 1

	v := fmt.Sprintf(
		"%c %c %s %s %s %c %d %s %c",
		markerGlobalSection,
		globalTypeSIndex,
		escapeASBS(sindex.Namespace),
		escapeASBS(sindex.Set),
		escapeASBS(sindex.Name),
		byte(sindex.IndexType),
		numPaths,
		escapeASBS(sindex.Path.BinName),
		byte(sindex.Path.BinType),
	)

	if sindex.Path.B64Context != "" {
		v = fmt.Sprintf("%s %s", v, sindex.Path.B64Context)
	}

	v += "\n"

	return []byte(v), nil
}
