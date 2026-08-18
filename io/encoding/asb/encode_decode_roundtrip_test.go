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
	"log/slog"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	particleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/require"
)

const roundTripTestFileName = "roundtrip_test.asb"

type roundTripCase struct {
	name    string
	compact bool
	record  *models.Record
}

func roundTripCases() []roundTripCase {
	stringKey, _ := a.NewKey("test", "roundtrip", "user-key")

	allTypesBins := a.BinMap{
		"bool_true":   true,
		"bool_false":  false,
		"int64_bin":   int64(9223372036854775807),
		"int32_bin":   int32(2147483647),
		"int16_bin":   int16(32000),
		"int8_bin":    int8(120),
		"int_bin":     123456789,
		"float64_bin": 123456.789123,
		"string_bin":  "text with spaces and symbols !@#$%^&*()",
		"bytes_bin":   []byte("raw-byte-payload-123"),
		"hll_bin":     a.HLLValue("hll-bytes"),
		"geojson_bin": a.GeoJSONValue(`{"type":"Point","coordinates":[12.34,56.78]}`),
		"nil_bin":     nil,
		"raw_map_bin": &a.RawBlobValue{
			ParticleType: particleType.MAP,
			Data:         []byte("raw-map-bytes"),
		},
		"raw_list_bin": &a.RawBlobValue{
			ParticleType: particleType.LIST,
			Data:         []byte("raw-list-bytes"),
		},
	}

	manyInts := a.BinMap{}
	for i := range 50 {
		manyInts[fmt.Sprintf("bin_%02d", i)] = int64(i * 1000)
	}

	floatEdgeBins := a.BinMap{
		"float_normal": 123.456,
		"float_long":   8.699637788021931e-151,
		"float_short":  2.000511e-212,
		"float_neg":    -9.799243036278548e-17,
		"float_big":    -2.490355e+26,
	}

	return []roundTripCase{
		{
			name:    "all_types_standard",
			compact: false,
			record:  roundTripRecord(stringKey, allTypesBins, 42, 1712345678),
		},
		{
			name:    "many_int_bins",
			compact: false,
			record:  roundTripRecord(stringKey, manyInts, 10, 1200),
		},
		{
			name:    "large_base64_payload",
			compact: false,
			record: roundTripRecord(
				stringKey,
				a.BinMap{"payload": bytes.Repeat([]byte("Z"), 65536)},
				3,
				500,
			),
		},
		{
			name:    "large_compact_payload",
			compact: true,
			record: roundTripRecord(
				stringKey,
				a.BinMap{"payload": bytes.Repeat([]byte("Z"), 65536)},
				3,
				500,
			),
		},
		{
			name:    "float_edge_cases",
			compact: false,
			record:  roundTripRecord(stringKey, floatEdgeBins, 7, 700),
		},
		{
			name:    "long_escaped_bin_name",
			compact: false,
			record: roundTripRecord(stringKey, a.BinMap{
				"this bin name is long enough to cache and contains spaces": "value",
			}, 2, 30),
		},
	}
}

func roundTripRecord(key *a.Key, bins a.BinMap, generation uint32, voidTime int64) *models.Record {
	return &models.Record{
		Record: &a.Record{
			Key:        key,
			Bins:       bins,
			Generation: generation,
		},
		VoidTime: voidTime,
	}
}

func TestEncodeDecodeRecordRoundTrip(t *testing.T) {
	t.Parallel()

	for _, tc := range roundTripCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			input := models.NewRecordToken(tc.record, 0, nil)
			got := encodeDecodeSingleRecord(t, tc.compact, input)
			assertRecordEqual(t, tc.record, got)
		})
	}
}

func encodeDecodeSingleRecord(t *testing.T, compact bool, token *models.Token) *models.Record {
	t.Helper()

	records := encodeDecodeRecords(t, compact, token)
	require.Len(t, records, 1)

	return records[0]
}

func encodeDecodeRecords(t *testing.T, compact bool, tokens ...*models.Token) []*models.Record {
	t.Helper()

	records, err := roundTripEncodeDecode(compact, tokens...)
	require.NoError(t, err)

	return records
}

func roundTripEncodeDecode(compact bool, tokens ...*models.Token) ([]*models.Record, error) {
	encoder := NewEncoder[*models.Token](NewEncoderConfig("test", compact, false))

	var payload bytes.Buffer
	if _, err := payload.Write(encoder.GetHeader(true)); err != nil {
		return nil, err
	}

	for _, token := range tokens {
		encoded, err := encoder.EncodeToken(token, nil)
		if err != nil {
			return nil, err
		}

		if _, err := payload.Write(encoded); err != nil {
			return nil, err
		}
	}

	decoder, err := NewDecoder[*models.Token](
		bytes.NewReader(payload.Bytes()),
		roundTripTestFileName,
		false,
		slog.Default(),
	)
	if err != nil {
		return nil, err
	}

	records := make([]*models.Record, 0, len(tokens))
	for {
		gotToken, nextErr := decoder.NextToken()
		if errors.Is(nextErr, io.EOF) {
			break
		}

		if nextErr != nil {
			return nil, nextErr
		}

		if gotToken.Type != models.TokenTypeRecord {
			return nil, fmt.Errorf("unexpected token type: %v", gotToken.Type)
		}

		records = append(records, gotToken.Record)
	}

	if len(records) != len(tokens) {
		return nil, fmt.Errorf("record count mismatch: got %d want %d", len(records), len(tokens))
	}

	return records, nil
}

func assertRecordEqual(t *testing.T, want, got *models.Record) {
	t.Helper()

	require.Equal(t, normalizeRecord(want), got)
}

// normalizeRecord shapes a record the way the ASB decoder returns it, so
// testify/require.Equal (reflect.DeepEqual under the hood) can compare records.
func normalizeRecord(r *models.Record) *models.Record {
	if r == nil {
		return nil
	}

	key := normalizeKey(r.Key)

	return &models.Record{
		Record: &a.Record{
			Key:        key,
			Bins:       normalizeBins(r.Bins),
			Generation: r.Generation,
		},
		VoidTime: r.VoidTime,
	}
}

func normalizeKey(key *a.Key) *a.Key {
	if key == nil {
		return nil
	}

	userKey := normalizeValue(key.Value().GetObject())
	normalized, err := a.NewKeyWithDigest(key.Namespace(), key.SetName(), userKey, key.Digest())
	if err != nil {
		panic(err)
	}

	return normalized
}

func normalizeBins(bins a.BinMap) a.BinMap {
	if len(bins) == 0 {
		return bins
	}

	out := make(a.BinMap, len(bins))
	for name, val := range bins {
		out[name] = normalizeValue(val)
	}

	return out
}

func normalizeValue(v any) any {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case int:
		return int64(val)
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case uint:
		return int64(val)
	case uint8:
		return int64(val)
	case uint16:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		return int64(val)
	case float32:
		return float64(val)
	case float64:
		return val
	case a.HLLValue:
		return a.NewHLLValue([]byte(val))
	case a.GeoJSONValue:
		return a.GeoJSONValue(string(val))
	case *a.RawBlobValue:
		return a.NewRawBlobValue(val.ParticleType, val.Data)
	default:
		return v
	}
}
