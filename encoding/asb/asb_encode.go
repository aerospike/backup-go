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
	"math"
	"time"

	"github.com/aerospike/backup-go/models"

	a "github.com/aerospike/aerospike-client-go/v7"
	particleType "github.com/aerospike/aerospike-client-go/v7/types/particle_type"
)

type Encoder struct{}

func NewEncoder() (*Encoder, error) {
	return &Encoder{}, nil
}

func (o *Encoder) EncodeToken(token *models.Token) ([]byte, error) {
	switch token.Type {
	case models.TokenTypeRecord:
		return o.EncodeRecord(token.Record)
	case models.TokenTypeUDF:
		return o.EncodeUDF(token.UDF)
	case models.TokenTypeSIndex:
		return o.EncodeSIndex(token.SIndex)
	case models.TokenTypeInvalid:
		return nil, errors.New("invalid token")
	default:
		return nil, fmt.Errorf("invalid token type: %v", token.Type)
	}
}

func (o *Encoder) EncodeRecord(rec *models.Record) ([]byte, error) {
	return recordToASB(rec)
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

// function pointer for time.Now to allow for testing
var getTimeNow = time.Now

func recordToASB(r *models.Record) ([]byte, error) {
	var data []byte

	if r == nil {
		return nil, errors.New("record is nil")
	}

	keyText, err := keyToASB(r.Key)
	if err != nil {
		return nil, err
	}

	data = append(data, keyText...)

	lineStart := []byte{markerRecordHeader, ' '}
	data = append(data, lineStart...)

	generationText := fmt.Sprintf("%c %d\n", recordHeaderTypeGen, r.Generation)
	data = append(data, generationText...)

	data = append(data, lineStart...)

	exprTime := getExpirationTime(r.Expiration, getTimeNow().Unix())
	expirationText := fmt.Sprintf("%c %d\n", recordHeaderTypeExpiration, exprTime)
	data = append(data, expirationText...)

	data = append(data, lineStart...)

	binCountText := fmt.Sprintf("%c %d\n", recordHeaderTypeBinCount, len(r.Bins))
	data = append(data, binCountText...)

	binsText, err := binsToASB(r.Bins)
	if err != nil {
		return nil, err
	}

	data = append(data, binsText...)

	return data, nil
}

func binsToASB(bins a.BinMap) ([]byte, error) {
	var res []byte

	if len(bins) < 1 {
		return res, fmt.Errorf("ERR: empty binmap")
	}

	// NOTE golang's random order map iteration
	// means that any backup files that include
	// multi element bin maps may not be identical
	// over multiple backups even if the data is the same
	for k, v := range bins {
		binText, err := binToASB(k, v)
		if err != nil {
			return nil, err
		}

		res = append(res, binText...)
	}

	return res, nil
}

func binToASB(k string, v any) ([]byte, error) {
	var res bytes.Buffer

	res.Write([]byte{markerRecordBins, ' '})

	binName := escapeASBS(k)

	switch v := v.(type) {
	case bool:
		res.Write([]byte(fmt.Sprintf("%c %s %c\n", binTypeBool, binName, boolToASB(v))))
	case int64, int32, int16, int8, int:
		res.Write([]byte(fmt.Sprintf("%c %s %d\n", binTypeInt, binName, v)))
	case float64:
		res.Write([]byte(fmt.Sprintf("%c %s %f\n", binTypeFloat, binName, v)))
	case string:
		res.Write([]byte(fmt.Sprintf("%c %s %d %s\n", binTypeString, binName, len(v), v)))
	case []byte:
		encoded := base64Encode(v)
		data := blobBinToASB([]byte(encoded), binTypeBytes, binName)
		res.Write(data)
	case *a.RawBlobValue:
		data, err := rawBlobBinToASB(v, k)
		if err != nil {
			return nil, err
		}

		res.Write(data)
	case map[any]any:
		return nil, errors.New("maps are only supported in raw blob bins")
	case []any:
		return nil, errors.New("lists are only supported in raw blob bins")
	case a.HLLValue:
		encoded := base64Encode(v)
		res.Write([]byte(fmt.Sprintf("%c %s %d %s\n", binTypeBytesHLL, binName, len(encoded), encoded)))
	case a.GeoJSONValue:
		res.Write([]byte(fmt.Sprintf("%c %s %d %s\n", binTypeGeoJSON, binName, len(v), v)))
	case nil:
		res.Write([]byte(fmt.Sprintf("%c %s\n", binTypeNil, binName)))
	default:
		return nil, fmt.Errorf("unknown bin type: %T, key: %s", v, k)
	}

	return res.Bytes(), nil
}

func rawBlobBinToASB(cdt *a.RawBlobValue, name string) ([]byte, error) {
	switch cdt.ParticleType {
	case particleType.MAP:
		return rawMapBinToASB(cdt, name), nil
	case particleType.LIST:
		return rawListBinToASB(cdt, name), nil
	default:
		return nil, fmt.Errorf("invalid raw blob bin particle type: %v", cdt.ParticleType)
	}
}

func rawMapBinToASB(cdt *a.RawBlobValue, name string) []byte {
	var res bytes.Buffer

	binName := escapeASBS(name)
	b64Bytes := base64Encode(cdt.Data)
	data := blobBinToASB([]byte(b64Bytes), binTypeBytesMap, binName)
	res.Write(data)

	return res.Bytes()
}

func rawListBinToASB(cdt *a.RawBlobValue, name string) []byte {
	var res bytes.Buffer

	binName := escapeASBS(name)
	b64Bytes := base64Encode(cdt.Data)
	data := blobBinToASB([]byte(b64Bytes), binTypeBytesList, binName)
	res.Write(data)

	return res.Bytes()
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

func keyToASB(k *a.Key) ([]byte, error) {
	var data []byte

	if k == nil {
		return nil, errors.New("key is nil")
	}

	userKeyText, err := userKeyToASB(k.Value())
	if err != nil {
		return nil, err
	}

	data = append(data, userKeyText...)

	lineStart := []byte{markerRecordHeader, ' '}
	data = append(data, lineStart...)

	namespaceText := fmt.Sprintf("%c %s\n", recordHeaderTypeNamespace, escapeASBS(k.Namespace()))
	data = append(data, namespaceText...)

	data = append(data, lineStart...)

	b64Digest := base64Encode(k.Digest())
	digestText := fmt.Sprintf("%c %s\n", recordHeaderTypeDigest, b64Digest)
	data = append(data, digestText...)

	if k.SetName() != "" {
		data = append(data, lineStart...)
		setnameText := fmt.Sprintf("%c %s\n", recordHeaderTypeSet, escapeASBS(k.SetName()))
		data = append(data, setnameText...)
	}

	return data, nil
}

func base64Encode(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func userKeyToASB(userKey a.Value) ([]byte, error) {
	var data bytes.Buffer

	// user key is optional
	if userKey == nil {
		return nil, nil
	}

	linStart := []byte{markerRecordHeader, ' ', recordHeaderTypeKey, ' '}
	data.Write(linStart)

	val := userKey.GetObject()
	switch v := val.(type) {
	case int64, int32, int16, int8, int:
		data.Write([]byte(fmt.Sprintf("%c %d\n", keyTypeInt, v)))
	case float64:
		data.Write([]byte(fmt.Sprintf("%c %f\n", keyTypeFloat, v)))
	case string:
		data.Write([]byte(fmt.Sprintf("%c %d %s\n", keyTypeString, len(v), v)))
	case []byte:
		encoded := base64Encode(v)
		data.Write([]byte(fmt.Sprintf("%c %d %s\n", keyTypeBytes, len(encoded), encoded)))
	default:
		return nil, fmt.Errorf("invalid user key type: %T", v)
	}

	return data.Bytes(), nil
}

func getExpirationTime(ttl uint32, unixNow int64) uint32 {
	// math.MaxUint32 (-1) means never expire
	if ttl == math.MaxUint32 {
		return 0
	}

	timeSinceCE := unixNow - citrusLeafEpoch

	return uint32(timeSinceCE) + ttl
}

// **** SINDEX ****

// control characters
var asbEscapedChars = map[byte]struct{}{
	'\\': {},
	' ':  {},
	'\n': {},
}

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
