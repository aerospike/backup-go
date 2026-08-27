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

package streamers

import (
	"encoding/json"
	"errors"
	"math/rand/v2"
	"strings"
	"testing"
)

// realManifest is shaped like the manifest a server writes, fields the
// validator does not care about included.
const realManifest = `{
  "backup_id": "test-remove",
  "namespace": "source-ns1",
  "partition_id": 935,
  "format_version": 1,
  "node_id": "BB951D8A16DC7A2",
  "checksum_algorithm": "crc32",
  "entry_count": 2,
  "update_time": 525347837866,
  "regime": 7,
  "flushed_digest": "a703155ccdd2c225f91e5c33d2e58961590d5687",
  "partition_complete": true,
  "segments": [
    {
      "segment_name": "test-remove/ns/source-ns1/query-stream/data/p935/7-0000181178352-db526a.seg",
      "size": 326752,
      "checksum": "c7905179",
      "pid": 935,
      "record_count": 216,
      "digest_hi": "a703155ccdd2c225f91e5c33d2e58961590d5687"
    },
    {
      "segment_name": "test-remove/ns/source-ns1/query-stream/data/p935/7-0000181198764-3a116c.seg",
      "size": 64128,
      "checksum": "43695c1e"
    }
  ],
  "trailing": {"nested": [1, 2, 3]}
}`

// decodeAll walks a manifest and collects the segments it records.
func decodeAll(t *testing.T, body string) (manifestHeader, []manifestSegment, error) {
	t.Helper()

	var (
		segments []manifestSegment
		header   manifestHeader
	)

	err := decodeManifest(strings.NewReader(body), &header, func(seg manifestSegment) error {
		segments = append(segments, seg)

		return nil
	})

	return header, segments, err
}

func TestDecodeManifest(t *testing.T) {
	t.Parallel()

	header, segments, err := decodeAll(t, realManifest)
	if err != nil {
		t.Fatalf("decodeManifest() error = %v", err)
	}

	if header.Namespace != "source-ns1" || header.Partition != "p935" {
		t.Errorf("header = %+v, want the namespace and partition of the manifest", header)
	}

	if len(segments) != 2 {
		t.Fatalf("decodeManifest() recorded %d segments, want 2", len(segments))
	}

	if segments[0].Size != 326752 || !strings.HasSuffix(segments[0].SegmentName, "7-0000181178352-db526a.seg") {
		t.Errorf("first segment = %+v, want the one the manifest names first", segments[0])
	}
}

func TestDecodeManifest_SegmentsAreWalkedNotCollected(t *testing.T) {
	t.Parallel()

	// A manifest of a busy partition records more segments than a caller wants
	// to hold, so a caller that keeps nothing costs nothing.
	var body strings.Builder

	body.WriteString(`{"namespace":"ns1","segments":[`)

	const recorded = 50_000

	for i := range recorded {
		if i > 0 {
			body.WriteString(",")
		}

		body.WriteString(`{"segment_name":"backup/ns/ns1/query-stream/data/p1/s.seg","size":1}`)
	}

	body.WriteString(`],"partition_id":1}`)

	var (
		seen   int
		header manifestHeader
	)

	err := decodeManifest(strings.NewReader(body.String()), &header, func(manifestSegment) error {
		seen++

		return nil
	})
	if err != nil {
		t.Fatalf("decodeManifest() error = %v", err)
	}

	if seen != recorded {
		t.Fatalf("walked %d segments, want %d", seen, recorded)
	}

	// The fields after the segment array are still read as fields.
	if header.Partition != "p1" {
		t.Errorf("header = %+v, want the partition written after the segments", header)
	}
}

func TestDecodeManifest_Rejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{name: "not json", body: "not a manifest"},
		{name: "not an object", body: `["segments"]`},
		{name: "truncated", body: `{"segments":[{"segment_name":"a.seg"`},
		{name: "segments not an array", body: `{"segments":{"segment_name":"a.seg"}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, _, err := decodeAll(t, tt.body); err == nil {
				t.Fatal("decodeManifest() succeeded, want an error")
			}
		})
	}
}

func TestDecodeManifest_StopsOnTheCaller(t *testing.T) {
	t.Parallel()

	errStop := errors.New("enough")

	var header manifestHeader

	err := decodeManifest(strings.NewReader(realManifest), &header, func(manifestSegment) error {
		return errStop
	})

	if !errors.Is(err, errStop) {
		t.Fatalf("decodeManifest() error = %v, want the error of the caller", err)
	}
}

func TestManifestSegment_Resolve(t *testing.T) {
	t.Parallel()

	const data = "519118324/ns/source-ns1/query-stream/data"

	tests := []struct {
		name      string
		recorded  string
		partition string
		want      string
		wantErr   bool
	}{
		{
			name:     "path from the root of the storage",
			recorded: data + "/p7/s0.seg",
			want:     data + "/p7/s0.seg",
		},
		{
			name:      "bare name in the partition of the manifest",
			recorded:  "s0.seg",
			partition: "p7",
			want:      data + "/p7/s0.seg",
		},
		{
			name:     "bare name in a stream without partitions",
			recorded: "s0.seg",
			want:     data + "/s0.seg",
		},
		{name: "no name", recorded: "  ", wantErr: true},
		{name: "leaving the storage", recorded: "../../etc/passwd", wantErr: true},
		{name: "absolute", recorded: "/etc/passwd", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := manifestSegment{SegmentName: tt.recorded}.resolve(data, tt.partition)

			if tt.wantErr {
				if !errors.Is(err, ErrManifestUnusable) {
					t.Fatalf("resolve() error = %v, want ErrManifestUnusable", err)
				}

				return
			}

			if err != nil {
				t.Fatalf("resolve() error = %v", err)
			}

			if got != tt.want {
				t.Errorf("resolve() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestQuotas(t *testing.T) {
	t.Parallel()

	wide := unit{partitions: make([]string, 4096)}
	narrow := unit{partitions: []string{""}}

	tests := []struct {
		name  string
		units []unit
		n     int
		want  []int
	}{
		{
			name:  "a sample smaller than the units covers the widest of them",
			units: []unit{narrow, narrow, wide},
			n:     2,
			want:  []int{1, 0, 1},
		},
		{
			name:  "most of the sample goes where most of the partitions are",
			units: []unit{narrow, wide},
			n:     100,
			want:  []int{12, 88},
		},
		{
			name:  "every unit keeps a quarter of an equal share",
			units: []unit{narrow, narrow, narrow, wide},
			n:     400,
			want:  []int{25, 25, 25, 325},
		},
		{
			name:  "one unit takes the whole sample",
			units: []unit{wide},
			n:     10,
			want:  []int{10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := quotas(tt.units, tt.n)

			total := 0
			for _, q := range got {
				total += q
			}

			if total != tt.n {
				t.Errorf("quotas() hands out %d of a sample of %d: %v", total, tt.n, got)
			}

			if len(got) != len(tt.want) {
				t.Fatalf("quotas() = %v, want %v", got, tt.want)
			}

			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("quotas() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestShare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		remaining, sources, want int
	}{
		{remaining: 10, sources: 1, want: 10},
		{remaining: 10, sources: 4, want: 3},
		{remaining: 3, sources: 3, want: 1},
		{remaining: 0, sources: 3, want: 0},
	}

	for _, tt := range tests {
		if got := share(tt.remaining, tt.sources); got != tt.want {
			t.Errorf("share(%d, %d) = %d, want %d", tt.remaining, tt.sources, got, tt.want)
		}
	}
}

func TestReservoir(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewPCG(1, 2))
	res := newReservoir[int](3, rnd)

	for i := range 2 {
		res.offer(i)
	}

	if len(res.result()) != 2 {
		t.Fatalf("a reservoir offered 2 values holds %d, want 2", len(res.result()))
	}

	for i := range 1000 {
		res.offer(i)
	}

	if len(res.result()) != 3 {
		t.Fatalf("a reservoir of 3 holds %d values", len(res.result()))
	}
}

func TestReservoirIsUniform(t *testing.T) {
	t.Parallel()

	const (
		values = 20
		picks  = 4
		rounds = 20_000
	)

	counts := make([]int, values)

	for round := range rounds {
		res := newReservoir[int](picks, rand.New(rand.NewPCG(uint64(round), 7)))

		for i := range values {
			res.offer(i)
		}

		for _, v := range res.result() {
			counts[v]++
		}
	}

	// Every value should come up in a picks/values share of the rounds.
	expected := float64(rounds) * picks / values

	for v, count := range counts {
		if diff := float64(count) - expected; diff < -expected/5 || diff > expected/5 {
			t.Errorf("value %d picked %d times, want about %.0f", v, count, expected)
		}
	}
}

func TestSeedForIsStable(t *testing.T) {
	t.Parallel()

	first, second := seedFor(42, 1), seedFor(42, 2)
	if first == second {
		t.Error("two pieces of work of the same run share a seed")
	}

	if again := seedFor(42, 1); again != first {
		t.Errorf("the same piece of work of the same run got two seeds: %d and %d", first, again)
	}

	if seedFor(1, 1) == seedFor(2, 1) {
		t.Error("two runs share the seed of a piece of work")
	}
}

func TestDecodeManifest_PartitionNamedDirectly(t *testing.T) {
	t.Parallel()

	// A manifest writes its partition as a number, which names the directory
	// holding it. One that names the directory itself is taken at its word.
	header, _, err := decodeAll(t, `{"partition_id":"node-a","segments":[]}`)
	if err != nil {
		t.Fatalf("decodeManifest() error = %v", err)
	}

	if header.Partition != "node-a" {
		t.Fatalf("partition = %q, want %q", header.Partition, "node-a")
	}
}

func TestDecodeManifest_RejectsBrokenFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{name: "field name is not a string", body: `{1:2}`},
		{name: "partition is unreadable", body: `{"partition_id":tru}`},
		{name: "field the manifest does not finish", body: `{"trailing":{"nested":`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, _, err := decodeAll(t, tt.body); err == nil {
				t.Fatal("decodeManifest() succeeded, want an error")
			}
		})
	}
}

func TestReadKey(t *testing.T) {
	t.Parallel()

	// readKey reads whatever field name comes next, so which one it is does
	// not matter beyond it being one a manifest writes.
	const field = "namespace"

	tests := []struct {
		name    string
		body    string
		wantKey string
		wantErr bool
	}{
		{name: "field name", body: `{"` + field + `":1}`, wantKey: field},
		{name: "nothing to read", body: ``, wantErr: true},
		{name: "not a field name", body: `["` + field + `"]`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dec := json.NewDecoder(strings.NewReader(tt.body))

			if tt.wantKey != "" {
				// Step over the opening brace, so the next token is the key.
				if err := expectDelim(dec, '{'); err != nil {
					t.Fatalf("expectDelim() error = %v", err)
				}
			}

			key, err := readKey(dec)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("readKey() = %q, want an error", key)
				}

				return
			}

			if err != nil {
				t.Fatalf("readKey() unexpected error: %v", err)
			}

			if key != tt.wantKey {
				t.Fatalf("readKey() = %q, want %q", key, tt.wantKey)
			}
		})
	}
}
