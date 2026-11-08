// Copyright 2024-2026 Aerospike, Inc.
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

	"github.com/aerospike/backup-go/pkg/server/segvalidator/segment"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// realManifest is shaped like the manifest a server writes, fields the
// validator does not care about included.
const realManifest = `{
  "backup_id": "test-remove",
  "namespace": "source-ns1",
  "partition_id": 935,
  "format_version": 1,
  "node_id": "BB951D8A16DC7A2",
  "checksum_algorithm": "crc32c",
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
	require.NoError(t, err)

	assert.Equal(t, "source-ns1", header.Namespace, "want the namespace of the manifest")
	assert.Equal(t, "p935", header.Partition, "want the partition of the manifest")

	require.Len(t, segments, 2)

	// The first segment is the one the manifest names first.
	if assert.NotNil(t, segments[0].Size, "first segment = %+v", segments[0]) {
		assert.Equal(t, int64(326752), *segments[0].Size, "first segment = %+v", segments[0])
	}

	assert.True(t, strings.HasSuffix(segments[0].SegmentName, "7-0000181178352-db526a.seg"),
		"first segment = %+v, want the one the manifest names first", segments[0])

	// The second segment records no count, which reads as zero.
	assert.Equal(t, int64(testRecordCount), segments[0].RecordCount, "record count of the first segment")
	assert.Zero(t, segments[1].RecordCount, "record count of the second segment")
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
	require.NoError(t, err)

	require.Equal(t, recorded, seen, "segments walked")

	// The fields after the segment array are still read as fields.
	assert.Equal(t, "p1", header.Partition, "want the partition written after the segments")
}

func TestDecodeManifest_Rejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{name: "nothing at all", body: ""},
		{name: "not json", body: "not a manifest"},
		{name: "not an object", body: `["segments"]`},
		{name: "truncated", body: `{"segments":[{"segment_name":"a.seg"`},
		{name: "segments not an array", body: `{"segments":{"segment_name":"a.seg"}}`},
		{
			// A manifest whose upload was interrupted records segments that
			// parse, and must not be read as one that recorded only those.
			name: "unfinished past the segments",
			body: `{"namespace":"ns1","segments":[{"segment_name":"a.seg","size":1}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := decodeAll(t, tt.body)
			require.Error(t, err)

			// A run reports an unusable manifest and carries on; anything else
			// it takes for a storage that stopped answering and gives up on.
			assert.ErrorIs(t, err, ErrManifestUnusable)
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

	require.ErrorIs(t, err, errStop, "want the error of the caller")

	// The error of the caller is not the manifest failing to be read: a run
	// stopped by a canceled context must not come out of it holding a report
	// of a corrupt backup.
	assert.NotErrorIs(t, err, ErrManifestUnusable, "want the error of the caller alone")
}

func TestManifestSegment_Resolve(t *testing.T) {
	t.Parallel()

	const (
		data       = "519118324/ns/source-ns1/query-stream/data"
		prefix     = "backups/daily"
		prefixData = prefix + "/" + data
	)

	tests := []struct {
		name      string
		recorded  string
		prefix    string
		data      string
		partition string
		want      string
		wantErr   bool
	}{
		{
			name:     "path from the root of the storage",
			recorded: data + "/p7/s0.seg",
			data:     data,
			want:     data + "/p7/s0.seg",
		},
		{
			name:     "path from the root of a backup written under a path",
			recorded: data + "/p7/s0.seg",
			prefix:   prefix,
			data:     prefixData,
			want:     prefixData + "/p7/s0.seg",
		},
		{
			name:      "bare name in the partition of the manifest",
			recorded:  "s0.seg",
			data:      data,
			partition: "p7",
			want:      data + "/p7/s0.seg",
		},
		{
			name:      "bare name in a backup written under a path",
			recorded:  "s0.seg",
			prefix:    prefix,
			data:      prefixData,
			partition: "p7",
			want:      prefixData + "/p7/s0.seg",
		},
		{
			name:     "bare name in a stream without partitions",
			recorded: "s0.seg",
			data:     data,
			want:     data + "/s0.seg",
		},
		{name: "no name", recorded: "  ", data: data, wantErr: true},
		{name: "leaving the storage", recorded: "../../etc/passwd", data: data, wantErr: true},
		{name: "leaving the path", recorded: "../../etc/passwd", prefix: prefix, data: prefixData, wantErr: true},
		{name: "absolute", recorded: "/etc/passwd", data: data, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := manifestSegment{SegmentName: tt.recorded}.resolve(tt.prefix, tt.data, tt.partition)

			if tt.wantErr {
				require.ErrorIs(t, err, ErrManifestUnusable)

				return
			}

			require.NoError(t, err)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestManifestSegment_Check(t *testing.T) {
	t.Parallel()

	size := int64(64)
	checksum := testChecksum
	// An empty checksum is still one the manifest wrote down: the server
	// takes it as a zero and fails the segment against it, not the entry.
	empty := ""

	tests := []struct {
		name    string
		give    manifestSegment
		wantErr bool
	}{
		{name: "size and checksum", give: manifestSegment{Size: &size, Checksum: &checksum}},
		{name: "empty checksum", give: manifestSegment{Size: &size, Checksum: &empty}},
		{name: "no size", give: manifestSegment{Checksum: &checksum}, wantErr: true},
		{name: "no checksum", give: manifestSegment{Size: &size}, wantErr: true},
		{name: "neither", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.give.check()

			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, ErrManifestUnusable)
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

			assert.Equal(t, tt.n, total, "quotas() hands out a different total than the sample: %v", got)

			require.Equal(t, tt.want, got)
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
		assert.Equal(t, tt.want, share(tt.remaining, tt.sources), "share(%d, %d)", tt.remaining, tt.sources)
	}
}

func TestReservoir(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewPCG(1, 2))
	res := newReservoir[int](3, rnd)

	for i := range 2 {
		res.offer(i)
	}

	require.Len(t, res.result(), 2, "a reservoir offered 2 values")

	for i := range 1000 {
		res.offer(i)
	}

	require.Len(t, res.result(), 3, "a reservoir of 3")
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
		assert.InDelta(t, expected, float64(count), expected/5, "value %d picked too often or too rarely", v)
	}
}

func TestSeedForIsStable(t *testing.T) {
	t.Parallel()

	first, second := seedFor(42, 1), seedFor(42, 2)
	assert.NotEqual(t, first, second, "two pieces of work of the same run share a seed")

	assert.Equal(t, first, seedFor(42, 1), "the same piece of work of the same run got two seeds")

	assert.NotEqual(t, seedFor(1, 1), seedFor(2, 1), "two runs share the seed of a piece of work")
}

func TestDecodeManifest_PartitionNamedDirectly(t *testing.T) {
	t.Parallel()

	// A manifest writes its partition as a number, which names the directory
	// holding it. One that names the directory itself is taken at its word.
	header, _, err := decodeAll(t, `{"partition_id":"node-a","segments":[]}`)
	require.NoError(t, err)

	require.Equal(t, "node-a", header.Partition)
}

func TestDecodeManifest_RejectsBrokenFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{name: "field name is not a string", body: `{1:2}`},
		{name: "partition is unreadable", body: `{"partition_id":tru}`},
		{name: "partition is neither a number nor a name", body: `{"partition_id":[935]}`},
		{name: "field the manifest does not finish", body: `{"trailing":{"nested":`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := decodeAll(t, tt.body)
			require.Error(t, err)

			assert.ErrorIs(t, err, ErrManifestUnusable)
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
				require.NoError(t, expectDelim(dec, '{'))
			}

			key, err := readKey(dec)

			if tt.wantErr {
				require.Error(t, err, "readKey() = %q", key)

				return
			}

			require.NoError(t, err)

			require.Equal(t, tt.wantKey, key)
		})
	}
}

func TestSegment_Partition(t *testing.T) {
	t.Parallel()

	const (
		testQueryData  = "519118324/ns/test/query-stream/data/"
		testChangeData = "519118324/ns/test/change-stream/BB951D8A16DC7A2/data/"
		testSegment    = "/7-0000181178352-db526a.seg"
		testPartition  = "p935"
		testManifests  = "519118324/ns/test/query-stream/manifest/"
		testManifestID = "-7-0000181178352-db526a.json"
		// testManifestPartition is a partition other than the one of the
		// directory, so a test tells which of the two was taken.
		testManifestPartition = "12"
	)

	queryPartition := testQueryData + testPartition + testSegment
	manifestOfPartition := testManifests + testManifestPartition + testManifestID

	tests := []struct {
		name   string
		give   Segment
		wantID int
		wantOK bool
	}{
		{
			name:   "query stream partition",
			give:   Segment{Stream: QueryStream, Path: queryPartition},
			wantID: 935,
			wantOK: true,
		},
		{
			name:   "manifest partition outranks the directory",
			give:   Segment{Stream: QueryStream, Path: queryPartition, Manifest: manifestOfPartition},
			wantID: 12,
			wantOK: true,
		},
		{
			name: "manifest the server skips assigns no partition",
			give: Segment{
				Stream:   QueryStream,
				Path:     queryPartition,
				Manifest: testManifests + "manifest.json",
			},
		},
		{
			name: "change stream manifest mixes partitions",
			give: Segment{
				Stream:   ChangeStream,
				Path:     testChangeData + testPartition + testSegment,
				Manifest: manifestOfPartition,
			},
		},
		{
			name:   "first partition",
			give:   Segment{Stream: QueryStream, Path: testQueryData + "p0" + testSegment},
			wantID: 0,
			wantOK: true,
		},
		{
			name:   "last partition",
			give:   Segment{Stream: QueryStream, Path: testQueryData + "p4095" + testSegment},
			wantID: segment.PartitionCount - 1,
			wantOK: true,
		},
		{
			name: "partition out of range",
			give: Segment{Stream: QueryStream, Path: testQueryData + "p4096" + testSegment},
		},
		{
			name: "leading zero",
			give: Segment{Stream: QueryStream, Path: testQueryData + "p01" + testSegment},
		},
		{
			name: "explicit sign",
			give: Segment{Stream: QueryStream, Path: testQueryData + "p+1" + testSegment},
		},
		{
			name: "negative id",
			give: Segment{Stream: QueryStream, Path: testQueryData + "p-1" + testSegment},
		},
		{
			name: "not a number",
			give: Segment{Stream: QueryStream, Path: testQueryData + "px" + testSegment},
		},
		{
			name: "no partition directory",
			give: Segment{Stream: QueryStream, Path: testQueryData + "loose.seg"},
		},
		{
			name: "change stream mixes partitions",
			give: Segment{Stream: ChangeStream, Path: testChangeData + testPartition + testSegment},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			id, ok := tt.give.Partition()
			assert.Equal(t, tt.wantID, id, "Partition() id")
			assert.Equal(t, tt.wantOK, ok, "Partition() ok")
		})
	}
}
