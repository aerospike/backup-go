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
	"fmt"
	"math"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// testChangeManifestDir is the manifest directory of the change stream of
	// testNode.
	testChangeManifestDir = testBackupID + "/ns/" + testNS + "/change-stream/" + testNode + "/manifest/"
	// testChangeManifestName formats the name the server gives a change stream
	// manifest, {regime}-{timestamp:013}-{uuid:06x}.json.
	testChangeManifestName = "%d-%013d-%06x.json"
)

func TestParseChangeManifest(t *testing.T) {
	t.Parallel()

	longNode := strings.Repeat("A", maxChangeNodeIDLen)

	tests := []struct {
		name        string
		give        string
		wantNode    string
		wantCapture changeCapture
		wantOK      bool
	}{
		{
			name:        "name the server writes",
			give:        testChangeManifestDir + "7-0001811970100-00a1b2.json",
			wantNode:    testNode,
			wantCapture: changeCapture{regime: 7, timestamp: 1811970100},
			wantOK:      true,
		},
		{
			// The separator after the timestamp is no conversion.
			name:        "nothing after the timestamp",
			give:        testChangeManifestDir + "7-12.json",
			wantNode:    testNode,
			wantCapture: changeCapture{regime: 7, timestamp: 12},
			wantOK:      true,
		},
		{
			name:        "regime wraps around an unsigned int",
			give:        testChangeManifestDir + "4294967297-1-0.json",
			wantNode:    testNode,
			wantCapture: changeCapture{regime: 1, timestamp: 1},
			wantOK:      true,
		},
		{
			name:        "regime saturates",
			give:        testChangeManifestDir + "99999999999999999999-1-0.json",
			wantNode:    testNode,
			wantCapture: changeCapture{regime: math.MaxUint32, timestamp: 1},
			wantOK:      true,
		},
		{
			name:        "longest node id",
			give:        testBackupID + "/ns/" + testNS + "/change-stream/" + longNode + "/manifest/1-2-3.json",
			wantNode:    longNode,
			wantCapture: changeCapture{regime: 1, timestamp: 2},
			wantOK:      true,
		},
		{
			name: "node id too long",
			give: testBackupID + "/ns/" + testNS + "/change-stream/" + longNode + "A/manifest/1-2-3.json",
		},
		{
			name: "empty node id",
			give: testBackupID + "/ns/" + testNS + "/change-stream//manifest/1-2-3.json",
		},
		{
			name: "manifest of the stream itself",
			give: testBackupID + "/ns/" + testNS + "/change-stream/manifest/1-2-3.json",
		},
		{
			name: "query stream manifest",
			give: testBackupID + "/ns/" + testNS + "/query-stream/manifest/1-2-3-4.json",
		},
		{
			name: "no timestamp",
			give: testChangeManifestDir + "7.json",
		},
		{
			name: "regime is not a number",
			give: testChangeManifestDir + "stray-0000181275309.json",
		},
		{
			name: "not a json file",
			give: testChangeManifestDir + "7-12-0.txt",
		},
		{
			name: "nested below the manifest directory",
			give: testChangeManifestDir + "old/7-12-0.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			node, capture, ok := parseChangeManifest(tt.give)

			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantNode, node)
			assert.Equal(t, tt.wantCapture, capture)
		})
	}
}

func TestChangeCaptureNewer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		give  changeCapture
		other changeCapture
		want  bool
	}{
		{name: "higher regime", give: changeCapture{regime: 2}, other: changeCapture{regime: 1, timestamp: 9}, want: true},
		{name: "lower regime", give: changeCapture{regime: 1, timestamp: 9}, other: changeCapture{regime: 2}},
		{name: "later in the same regime", give: changeCapture{regime: 1, timestamp: 2}, other: changeCapture{regime: 1, timestamp: 1}, want: true},
		{name: "earlier in the same regime", give: changeCapture{regime: 1, timestamp: 1}, other: changeCapture{regime: 1, timestamp: 2}},
		{name: "same capture", give: changeCapture{regime: 1, timestamp: 1}, other: changeCapture{regime: 1, timestamp: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.give.newer(tt.other))
		})
	}
}

// changeNodeWithHistory writes the change stream of testNode as a node that
// restarted its change stream: an old manifest of an earlier regime, an old
// manifest of the current regime, and the newest one. Each records segments of
// its own. It returns the segments of the newest manifest and of the old ones.
func changeNodeWithHistory(b *testBackup) (newest, old []string) {
	root := path.Join(b.id, namespacesDir, testNS, string(ChangeStream), testNode)

	manifests := []struct {
		regime    int
		timestamp int
		newest    bool
	}{
		{regime: 1, timestamp: 900},
		{regime: 2, timestamp: 100, newest: true},
		{regime: 2, timestamp: 50},
	}

	for i, m := range manifests {
		segPath := path.Join(root, dataDir, fmt.Sprintf("%d-%013d-%06x.seg", m.regime, m.timestamp, i))
		body := segmentBody(128 + i)

		b.put(segPath, body)
		b.put(path.Join(root, manifestDir, fmt.Sprintf(testChangeManifestName, m.regime, m.timestamp, i)),
			testManifest(testNS, -1, []testSegment{{name: segPath, size: int64(len(body))}}))

		if m.newest {
			newest = append(newest, segPath)
		} else {
			old = append(old, segPath)
		}
	}

	return newest, old
}

// A node is restored from its newest change stream manifest alone. What the
// others recorded is no part of the backup a restore reads.
func TestStreamAll_ChangeStreamIsReadFromTheNewestManifest(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 1, 1, 0, 0)
	newest, old := changeNodeWithHistory(b)

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)
			segments := collect(t, func(out chan<- Segment) error { return s.StreamAll(t.Context(), out) })

			var recorded, unrecorded []string

			for _, seg := range segments {
				if seg.Stream != ChangeStream {
					continue
				}

				if seg.Unrecorded {
					unrecorded = append(unrecorded, seg.Path)
				} else {
					recorded = append(recorded, seg.Path)
				}
			}

			assert.Equal(t, newest, recorded)
			assert.ElementsMatch(t, old, unrecorded)

			stats := s.Stats()
			assert.EqualValues(t, 4, stats.ManifestsFound)
			assert.EqualValues(t, 2, stats.ManifestsRead)
			assert.EqualValues(t, 2, stats.ManifestsSuperseded)
			assert.Zero(t, stats.ManifestsFailed)
		})
	}
}

func TestStreamSample_ChangeStreamIsSampledFromTheNewestManifest(t *testing.T) {
	t.Parallel()

	b := newTestBackup(testBackupID)
	newest, _ := changeNodeWithHistory(b)

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)
			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamSample(t.Context(), 10, out)
			})

			assert.Equal(t, newest, paths(segments))

			stats := s.Stats()
			assert.EqualValues(t, 1, stats.ManifestsRead)
			assert.EqualValues(t, 2, stats.ManifestsSuperseded)
		})
	}
}

// A change stream manifest the server cannot read the name of is one it skips,
// so what it records is never restored.
func TestStreaming_ChangeManifestTheServerSkipsIsReported(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 1, 1, 1, 1)
	b.put(testChangeManifestDir+"broken.json", testManifest(testNS, -1, nil))

	for name, run := range streamRuns(t.Context(), 10) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			writeBackup(t, root, b)
			s := newTestStreamer(t, &localStore{root: root})

			collect(t, func(out chan<- Segment) error { return run(s, out) })

			stats := s.Stats()
			require.Len(t, stats.ManifestIssues, 1)
			require.ErrorIs(t, stats.ManifestIssues[0].Err, ErrManifestUnusable)
			assert.Equal(t, testChangeManifestDir+"broken.json", stats.ManifestIssues[0].Path)
		})
	}
}

// The server restores the change streams of a bounded number of nodes, which
// are counted by the manifests a restore reads.
func TestStreamAll_TooManyChangeStreamNodes(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 1, 1, 0, 0)

	for n := range maxChangeNodes + 1 {
		b.changeNode(testNS, fmt.Sprintf("BB951D8A16D%04X", n), 0)
	}

	root := t.TempDir()
	writeBackup(t, root, b)
	s := newTestStreamer(t, &localStore{root: root})

	collect(t, func(out chan<- Segment) error { return s.StreamAll(t.Context(), out) })

	stats := s.Stats()
	require.Len(t, stats.ManifestIssues, 1)
	require.ErrorIs(t, stats.ManifestIssues[0].Err, ErrManifestUnusable)
	assert.Equal(t, streamRoot(testBackupID, testNS, ChangeStream), stats.ManifestIssues[0].Path)
}

// A node holding no manifest whose name the server reads has no change stream
// a restore reads, so it does not count against the bound.
func TestStreamAll_NodeWithoutAReadableManifestIsNotCounted(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 1, 1, 0, 0)

	for n := range maxChangeNodes {
		b.changeNode(testNS, fmt.Sprintf("BB951D8A16D%04X", n), 0)
	}

	strayNode := path.Join(testBackupID, namespacesDir, testNS, string(ChangeStream), "BB951D8A16DFFFF")
	b.put(path.Join(strayNode, manifestDir, "broken.json"), testManifest(testNS, -1, nil))

	root := t.TempDir()
	writeBackup(t, root, b)
	s := newTestStreamer(t, &localStore{root: root})

	collect(t, func(out chan<- Segment) error { return s.StreamAll(t.Context(), out) })

	stats := s.Stats()
	require.Len(t, stats.ManifestIssues, 1)
	assert.Equal(t, path.Join(strayNode, manifestDir, "broken.json"), stats.ManifestIssues[0].Path)
}

// A sample bounds the listing of a change stream like any other, and picks the
// newest of the manifests it looked at.
func TestStreamSample_ChangeManifestListingIsBounded(t *testing.T) {
	t.Parallel()

	b := newTestBackup(testBackupID)
	changeNodeWithHistory(b)

	root := t.TempDir()
	writeBackup(t, root, b)
	s := newTestStreamer(t, &localStore{root: root}, WithScanLimit(1))

	segments := collect(t, func(out chan<- Segment) error { return s.StreamSample(t.Context(), 10, out) })

	assert.Len(t, segments, 1)

	stats := s.Stats()
	assert.EqualValues(t, 1, stats.ManifestsFound)
	assert.EqualValues(t, 1, stats.ManifestsRead)
	assert.Zero(t, stats.ManifestsSuperseded)
}
