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

	"github.com/aerospike/aerospike-tools-backup-lib/models"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type Encoder struct{}

func NewEncoder() (*Encoder, error) {
	return &Encoder{}, nil
}

func (o *Encoder) EncodeRecord(rec *models.Record) ([]byte, error) {
	return recordToASB(rec)
}

func (o *Encoder) EncodeUDF(udf *models.UDF) ([]byte, error) {
	return nil, fmt.Errorf("%w: unimplemented", errors.ErrUnsupported)
}

func (o *Encoder) EncodeSIndex(sindex *models.SIndex) ([]byte, error) {
	return _SIndexToASB(sindex)
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

	generationText := fmt.Sprintf("%c g %d\n", markerRecordHeader, r.Generation)
	data = append(data, generationText...)

	exprTime := getExpirationTime(r.Expiration, getTimeNow().Unix())
	expirationText := fmt.Sprintf("%c t %d\n", markerRecordHeader, exprTime)
	data = append(data, expirationText...)

	binCountText := fmt.Sprintf("%c b %d\n", markerRecordHeader, len(r.Bins))
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
		res.Write([]byte(fmt.Sprintf("Z %s %c\n", binName, boolToASB(v))))
	case int64, int32, int16, int8, int:
		res.Write([]byte(fmt.Sprintf("I %s %d\n", binName, v)))
	case float64:
		res.Write([]byte(fmt.Sprintf("D %s %f\n", binName, v)))
	case string:
		res.Write([]byte(fmt.Sprintf("S %s %d %s\n", binName, len(v), v)))
	case a.HLLValue:
		encoded := base64Encode(v)
		res.Write([]byte(fmt.Sprintf("Y %s %d %s\n", binName, len(encoded), encoded)))
	case []byte:
		encoded := base64Encode(v)
		res.Write([]byte(fmt.Sprintf("B %s %d %s\n", binName, len(encoded), encoded)))
	case map[any]any:
		return nil, errors.New("map bin not supported")
	case []any:
		return nil, errors.New("list bin not supported")
	case nil:
		res.Write([]byte(fmt.Sprintf("N %s\n", binName)))
	default:
		return nil, fmt.Errorf("unknown user key type: %T, key: %s", v, k)
	}

	return res.Bytes(), nil
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
		return data, err
	}
	data = append(data, userKeyText...)

	namespaceText := fmt.Sprintf("%c n %s\n", markerRecordHeader, escapeASBS(k.Namespace()))
	data = append(data, namespaceText...)

	b64Digest := base64Encode(k.Digest())
	digestText := fmt.Sprintf("%c d %s\n", markerRecordHeader, b64Digest)
	data = append(data, digestText...)

	if k.SetName() != "" {
		setnameText := fmt.Sprintf("%c s %s\n", markerRecordHeader, escapeASBS(k.SetName()))
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

	val := userKey.GetObject()

	switch v := val.(type) {
	case int64, int32, int16, int8, int:
		data.Write([]byte(fmt.Sprintf("%c k I %d\n", markerRecordHeader, v)))
	case float64:
		data.Write([]byte(fmt.Sprintf("%c k D %f\n", markerRecordHeader, v)))
	case string:
		data.Write([]byte(fmt.Sprintf("%c k S %d %s\n", markerRecordHeader, len(v), v)))
	case []byte:
		encoded := base64Encode(v)
		data.Write([]byte(fmt.Sprintf("%c k B %d %s\n", markerRecordHeader, len(encoded), encoded)))
	default:
		return nil, fmt.Errorf("invalid user key type: %T", v)
	}

	return data.Bytes(), nil
}

func getExpirationTime(ttl uint32, unix_now int64) uint32 {
	// math.MaxUint32 (-1) means never expire
	if ttl == math.MaxUint32 {
		return 0
	}

	timeSinceCE := unix_now - citrusLeafEpoch
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

func _SIndexToASB(sindex *models.SIndex) ([]byte, error) {
	if sindex == nil {
		return nil, errors.New("sindex is nil")
	}

	// sindexes only ever use 1 path for now
	numPaths := 1

	v := fmt.Sprintf(
		"%c %c %s %s %s %c %d %s %c",
		markerGlobalSection,
		'i',
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
