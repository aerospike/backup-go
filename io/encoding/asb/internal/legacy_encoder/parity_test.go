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

package legacy_encoder_test

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	particleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/encoding/asb/internal/legacy_encoder"
	"github.com/aerospike/backup-go/models"
)

type recordEncoderWorkload struct {
	name    string
	compact bool
	record  *models.Record
}

func recordEncoderWorkloads() []recordEncoderWorkload {
	key, _ := a.NewKey("test", "demo", "benchmark-user-key")
	escapedKey, _ := a.NewKey("test\nnamespace", "set name", "key with spaces")

	allTypes := a.BinMap{
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

	return []recordEncoderWorkload{
		{
			name:    "small_scalar",
			compact: false,
			record:  recordEncoderFixture(key, a.BinMap{"only": 42}, 1, 99),
		},
		{
			name:    "medium_all_types",
			compact: false,
			record:  recordEncoderFixture(key, allTypes, 42, 1712345678),
		},
		{
			name:    "medium_all_types_compact",
			compact: true,
			record:  recordEncoderFixture(key, allTypes, 42, 1712345678),
		},
		{
			name:    "large_base64_64k",
			compact: false,
			record:  recordEncoderFixture(key, a.BinMap{"payload": bytes.Repeat([]byte("Z"), 65536)}, 3, 500),
		},
		{
			name:    "large_compact_64k",
			compact: true,
			record:  recordEncoderFixture(key, a.BinMap{"payload": bytes.Repeat([]byte("Z"), 65536)}, 3, 500),
		},
		{
			name:    "many_int_bins",
			compact: false,
			record:  recordEncoderFixture(key, manyInts, 10, 1200),
		},
		{
			name:    "escaped_metadata",
			compact: false,
			record:  recordEncoderFixture(escapedKey, a.BinMap{"bin one": "hello"}, 1234, 10),
		},
		{
			name:    "long_escaped_names",
			compact: false,
			record: recordEncoderFixture(key, a.BinMap{
				"this bin name is long enough to cache and contains spaces": "value",
			}, 2, 30),
		},
	}
}

func recordEncoderFixture(key *a.Key, bins a.BinMap, generation uint32, voidTime int64) *models.Record {
	return &models.Record{
		Record: &a.Record{
			Key:        key,
			Bins:       bins,
			Generation: generation,
		},
		VoidTime: voidTime,
	}
}

func TestRecordEncoderMatchesLegacy(t *testing.T) {
	t.Parallel()

	for _, workload := range recordEncoderWorkloads() {
		t.Run(workload.name, func(t *testing.T) {
			t.Parallel()
			assertRecordEncoderMatchesLegacy(t, workload)
		})
	}
}

func TestRecordEncoderPreservesPartialOutputOnError(t *testing.T) {
	t.Parallel()

	key, err := a.NewKey("test", "demo", "key")
	if err != nil {
		t.Fatal(err)
	}
	record := recordEncoderFixture(key, a.BinMap{"bad": struct{}{}}, 1, 2)

	legacyBuf := bytes.NewBufferString("existing:")
	legacyN, legacyErr := legacy_encoder.RecordToASB(false, record, legacyBuf)

	encoder := asb.NewEncoder[*models.Token](asb.NewEncoderConfig("test", false, false))
	token := &models.Token{Type: models.TokenTypeRecord, Record: record}
	currentOut, currentErr := encoder.EncodeToken(token, []byte("existing:"))

	if legacyErr == nil || currentErr == nil {
		t.Fatalf("expected errors: legacy=%v current=%v", legacyErr, currentErr)
	}
	if !strings.Contains(currentErr.Error(), legacyErr.Error()) {
		t.Fatalf("error mismatch: legacy=%v current=%v", legacyErr, currentErr)
	}
	if legacyN != len(currentOut)-len("existing:") {
		t.Fatalf("bytes written mismatch: legacy=%d current=%d", legacyN, len(currentOut)-len("existing:"))
	}
	if !bytes.Equal(legacyBuf.Bytes(), currentOut) {
		t.Fatalf("partial output mismatch:\nlegacy=%q\ncurrent=%q", legacyBuf.Bytes(), currentOut)
	}
}

func TestRecordEncoderMetadataCacheHandlesNamespaceAndSetChanges(t *testing.T) {
	t.Parallel()

	keyA, err := a.NewKey("namespace_a", "set_a", "key")
	if err != nil {
		t.Fatal(err)
	}
	keyB, err := a.NewKey("namespace_b", "set_b", "key")
	if err != nil {
		t.Fatal(err)
	}
	records := []*models.Record{
		recordEncoderFixture(keyA, a.BinMap{"value": 1}, 1, 10),
		recordEncoderFixture(keyB, a.BinMap{"value": 2}, 2, 20),
		recordEncoderFixture(keyA, a.BinMap{"value": 3}, 3, 30),
	}

	encoder := asb.NewEncoder[*models.Token](asb.NewEncoderConfig("test", false, false))
	for _, record := range records {
		legacy := &bytes.Buffer{}
		if _, err := legacy_encoder.RecordToASB(false, record, legacy); err != nil {
			t.Fatal(err)
		}

		token := &models.Token{Type: models.TokenTypeRecord, Record: record}
		currentOut, err := encoder.EncodeToken(token, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(legacy.Bytes(), currentOut) {
			t.Fatalf("output mismatch:\nlegacy=%q\ncurrent=%q", legacy.Bytes(), currentOut)
		}
	}
}

func TestEncodeTokenRecordMatchesLegacy(t *testing.T) {
	t.Parallel()

	for _, workload := range recordEncoderWorkloads() {
		t.Run(workload.name, func(t *testing.T) {
			t.Parallel()

			legacy := &bytes.Buffer{}
			if _, err := legacy_encoder.RecordToASB(workload.compact, workload.record, legacy); err != nil {
				t.Fatal(err)
			}

			cfg := asb.NewEncoderConfig("test", workload.compact, false)
			encoder := asb.NewEncoder[*models.Token](cfg)
			token := &models.Token{Type: models.TokenTypeRecord, Record: workload.record}

			out, err := encoder.EncodeToken(token, nil)
			if err != nil {
				t.Fatal(err)
			}

			assertBytesMatchLegacy(t, legacy.Bytes(), out, len(workload.record.Bins))
		})
	}
}

func TestEncodeTokenRecordParity(t *testing.T) {
	t.Parallel()

	encoder := asb.NewEncoder[*models.Token](asb.NewEncoderConfig("test", false, false))

	key, keyErr := a.NewKey("test", "demo", "1234")
	if keyErr != nil {
		t.Fatal(keyErr)
	}

	token := &models.Token{
		Type: models.TokenTypeRecord,
		Record: &models.Record{
			Record: &a.Record{
				Key:  key,
				Bins: a.BinMap{"bin1": 0},
			},
		},
	}

	legacy := &bytes.Buffer{}
	if _, err := legacy_encoder.RecordToASB(false, token.Record, legacy); err != nil {
		t.Fatal(err)
	}

	actual, encodeErr := encoder.EncodeToken(token, nil)
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	if !bytes.Equal(legacy.Bytes(), actual) {
		t.Fatalf("output mismatch:\nlegacy=%q\ncurrent=%q", legacy.Bytes(), actual)
	}
}

func BenchmarkRecordEncoderWorkloads(b *testing.B) {
	for _, workload := range recordEncoderWorkloads() {
		legacy := &bytes.Buffer{}
		if _, err := legacy_encoder.RecordToASB(workload.compact, workload.record, legacy); err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(legacy.Len()))

		b.Run(workload.name+"/Legacy", func(b *testing.B) {
			benchmarkLegacyRecordEncoder(b, workload)
		})
		b.Run(workload.name+"/EncodeToken", func(b *testing.B) {
			benchmarkEncodeTokenRecord(b, workload)
		})
	}
}

func assertRecordEncoderMatchesLegacy(t *testing.T, workload recordEncoderWorkload) {
	t.Helper()

	legacy := &bytes.Buffer{}
	legacyN, legacyErr := legacy_encoder.RecordToASB(workload.compact, workload.record, legacy)

	encoder := asb.NewEncoder[*models.Token](asb.NewEncoderConfig("test", workload.compact, false))
	token := &models.Token{Type: models.TokenTypeRecord, Record: workload.record}
	currentOut, currentErr := encoder.EncodeToken(token, nil)

	if fmt.Sprint(legacyErr) != fmt.Sprint(currentErr) {
		t.Fatalf("error mismatch: legacy=%v current=%v", legacyErr, currentErr)
	}
	if legacyN != len(currentOut) {
		t.Fatalf("bytes written mismatch: legacy=%d current=%d", legacyN, len(currentOut))
	}

	assertBytesMatchLegacy(t, legacy.Bytes(), currentOut, len(workload.record.Bins))
}

func assertBytesMatchLegacy(t *testing.T, legacy, current []byte, binCount int) {
	t.Helper()

	if binCount == 1 {
		if !bytes.Equal(legacy, current) {
			t.Fatalf("output mismatch:\nlegacy=%q\ncurrent=%q", legacy, current)
		}
		return
	}

	if !bytes.Equal(sortBinOutput(string(legacy)), sortBinOutput(string(current))) {
		t.Fatalf("output mismatch:\nlegacy=%q\ncurrent=%q", legacy, current)
	}
}

func benchmarkLegacyRecordEncoder(b *testing.B, workload recordEncoderWorkload) {
	b.Helper()
	b.ReportAllocs()
	out := &bytes.Buffer{}
	for b.Loop() {
		out.Reset()
		if _, err := legacy_encoder.RecordToASB(workload.compact, workload.record, out); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkEncodeTokenRecord(b *testing.B, workload recordEncoderWorkload) {
	b.Helper()
	b.ReportAllocs()
	cfg := asb.NewEncoderConfig("test", workload.compact, false)
	encoder := asb.NewEncoder[*models.Token](cfg)
	token := &models.Token{Type: models.TokenTypeRecord, Record: workload.record}
	out := make([]byte, 0, 4096)
	for b.Loop() {
		out = out[:0]
		var err error
		out, err = encoder.EncodeToken(token, out)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func sortBinOutput(s string) []byte {
	var sorted sort.StringSlice = strings.Split(s, "\n")
	sorted.Sort()
	return []byte(strings.Join(sorted, "\n"))
}
