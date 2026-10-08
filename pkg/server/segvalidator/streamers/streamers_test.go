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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
)

const (
	testBackupID = "519118324"
	testNS       = "source-ns1"
	testNode     = "BB951D8A16DC7A2"
	// testChecksum is what the manifests of a test backup record for every
	// segment they name.
	testChecksum = "c7905179"
	// testRecordCount is what the query stream manifests of a test backup
	// record as the record count of every segment they name.
	testRecordCount = 216
	// The fields of a manifest the tests write and break.
	manifestSegmentsKey = "segments"
	manifestSizeKey     = "size"
	manifestChecksumKey = "checksum"
	// testManifestName formats the name the server gives the manifest of a
	// query stream partition, {pid}-{regime}-{timestamp}-{uuid}.json.
	testManifestName = "%d-7-0000181197010-%06x.json"

	msgWantManifestUnusable = "issue error = %v, want ErrManifestUnusable"
)

// testBackup describes the files of a backup, which a test materializes into a
// directory, a bucket, or both.
type testBackup struct {
	files map[string][]byte
	id    string
	// recorded are the segments the manifests of the backup name, which is not
	// every segment it holds: a segment can sit in a data directory without any
	// manifest knowing about it.
	recorded []string
}

func newTestBackup(id string) *testBackup {
	return &testBackup{id: id, files: make(map[string][]byte)}
}

// recordedSegments are the segments the manifests name, sorted.
func (b *testBackup) recordedSegments() []string {
	found := slices.Clone(b.recorded)
	sort.Strings(found)

	return found
}

// put stores one file of the backup.
func (b *testBackup) put(storagePath string, body []byte) {
	b.files[storagePath] = body
}

// remove drops one file, which is how a test breaks a backup.
func (b *testBackup) remove(storagePath string) {
	delete(b.files, storagePath)
}

// queryPartition writes one partition of the query stream: its segments and the
// manifest recording them.
func (b *testBackup) queryPartition(namespace string, partition, segments int) {
	root := path.Join(b.id, namespacesDir, namespace, string(QueryStream))
	recorded := make([]testSegment, 0, segments)

	for i := range segments {
		name := fmt.Sprintf("7-%010d-%06x.seg", partition*100+i, partition)
		segPath := path.Join(root, dataDir, fmt.Sprintf("p%d", partition), name)
		body := segmentBody(64 + i)

		b.put(segPath, body)
		b.recorded = append(b.recorded, segPath)
		recorded = append(recorded, testSegment{name: segPath, size: int64(len(body))})
	}

	b.put(path.Join(root, manifestDir, fmt.Sprintf(testManifestName, partition, partition)),
		testManifest(namespace, partition, recorded))
}

// changeNode writes one node of the change stream, whose segments sit in its
// data directory instead of being grouped by partition.
func (b *testBackup) changeNode(namespace, node string, segments int) {
	root := path.Join(b.id, namespacesDir, namespace, string(ChangeStream), node)
	recorded := make([]testSegment, 0, segments)

	for i := range segments {
		segPath := path.Join(root, dataDir, fmt.Sprintf("0-%010d-%s.seg", i, node[:6]))
		body := segmentBody(128 + i)

		b.put(segPath, body)
		b.recorded = append(b.recorded, segPath)
		recorded = append(recorded, testSegment{name: segPath, size: int64(len(body))})
	}

	b.put(path.Join(root, manifestDir, "0-0000181275309.json"), testManifest(namespace, -1, recorded))
}

// streamRuns are the two ways a backup is read, keyed by the name of the
// subtest each runs in. sample is the size of the sample drawn.
func streamRuns(ctx context.Context, sample int) map[string]func(*Streamer, chan<- Segment) error {
	return map[string]func(*Streamer, chan<- Segment) error{
		"sample": func(s *Streamer, out chan<- Segment) error {
			return s.StreamSample(ctx, sample, out)
		},
		"all": func(s *Streamer, out chan<- Segment) error {
			return s.StreamAll(ctx, out)
		},
	}
}

// manifestPath is where the manifest of a query stream partition sits.
func (b *testBackup) manifestPath(partition int) string {
	return path.Join(b.id, namespacesDir, testNS, string(QueryStream), manifestDir,
		fmt.Sprintf(testManifestName, partition, partition))
}

// segments are the paths of every segment of the backup, sorted.
func (b *testBackup) segments() []string {
	var paths []string

	for p := range b.files {
		if isSegment(p) {
			paths = append(paths, p)
		}
	}

	sort.Strings(paths)

	return paths
}

// testSegment is a segment a test manifest records.
type testSegment struct {
	name string
	size int64
}

// testManifest writes a manifest the way a server writes one: the segments it
// records are named by their full path, and the fields around them are there to
// be walked over. A partition below zero is one the manifest does not name, as
// a change stream manifest does not; like one, it records no record counts.
func testManifest(namespace string, partition int, segments []testSegment) []byte {
	recorded := make([]map[string]any, 0, len(segments))

	for _, seg := range segments {
		entry := map[string]any{
			"segment_name":      seg.name,
			manifestSizeKey:     seg.size,
			manifestChecksumKey: testChecksum,
		}

		if partition >= 0 {
			entry["record_count"] = testRecordCount
		}

		recorded = append(recorded, entry)
	}

	m := map[string]any{
		"backup_id":          testBackupID,
		"namespace":          namespace,
		"format_version":     1,
		"node_id":            testNode,
		"checksum_algorithm": "crc32c",
		"entry_count":        len(segments),
		manifestSegmentsKey:  recorded,
		"partition_complete": true,
	}

	if partition >= 0 {
		m["partition_id"] = partition
	}

	body, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}

	return body
}

// segmentBody makes a segment payload of the given size. Nothing in this
// package looks inside a segment, only at how big it is.
func segmentBody(size int) []byte {
	return bytes.Repeat([]byte{0xab}, size)
}

// newTestBackupTree builds a backup of one namespace: partitions of a query
// stream, and nodes of a change stream.
func newTestBackupTree(t *testing.T, partitions, segmentsPerPartition, nodes, segmentsPerNode int) *testBackup {
	t.Helper()

	b := newTestBackup(testBackupID)

	for p := range partitions {
		b.queryPartition(testNS, p, segmentsPerPartition)
	}

	for n := range nodes {
		b.changeNode(testNS, fmt.Sprintf("BB951D8A16DC7%02X", n), segmentsPerNode)
	}

	return b
}

// countingStore counts what a run asks of a storage, so a test can prove that
// sampling downloads the manifests it samples from and nothing else.
type countingStore struct {
	store
	listedDirs  atomic.Int64
	listedFiles atomic.Int64
	opened      atomic.Int64
	openedPaths sync.Map
}

func (c *countingStore) listLevel(ctx context.Context, dir string, fn func(levelEntry) error) error {
	c.listedDirs.Add(1)

	return c.store.listLevel(ctx, dir, fn)
}

func (c *countingStore) listFiles(ctx context.Context, dir string, fn func(file) error) error {
	c.listedFiles.Add(1)

	return c.store.listFiles(ctx, dir, fn)
}

func (c *countingStore) open(ctx context.Context, storagePath string) (io.ReadCloser, error) {
	c.opened.Add(1)
	c.openedPaths.Store(storagePath, true)

	return c.store.open(ctx, storagePath)
}

// storeCase is one storage a test runs against.
type storeCase struct {
	store *countingStore
	name  string
}

// storesOf materializes a backup into every storage this package supports, so
// that one test proves both of them behave the same.
func storesOf(t *testing.T, b *testBackup) []storeCase {
	t.Helper()

	root := t.TempDir()
	writeBackup(t, root, b)

	return []storeCase{
		{name: "local", store: &countingStore{store: &localStore{root: root}}},
		{name: "s3", store: &countingStore{store: &s3Store{client: newFakeBucket(b.files), bucket: testBucket}}},
	}
}

// newTestStreamer creates a streamer over a storage a test built.
func newTestStreamer(t *testing.T, st store, opts ...Option) *Streamer {
	t.Helper()

	opts = append([]Option{WithSeed(1)}, opts...)

	s, err := newStreamer(st, testBackupID, opts...)
	if err != nil {
		t.Fatalf("newStreamer() error = %v", err)
	}

	return s
}

// collect drains a streaming run into a slice.
func collect(t *testing.T, run func(chan<- Segment) error) []Segment {
	t.Helper()

	out := make(chan Segment, 16)
	done := make(chan error, 1)

	go func() { done <- run(out) }()

	var segments []Segment
	for seg := range out {
		segments = append(segments, seg)
	}

	if err := <-done; err != nil {
		t.Fatalf("streaming failed: %v", err)
	}

	return segments
}

// paths are the paths of the streamed segments, sorted.
func paths(segments []Segment) []string {
	found := make([]string, 0, len(segments))

	for _, seg := range segments {
		found = append(found, seg.Path)
	}

	sort.Strings(found)

	return found
}

func TestStreamAll_FindsEverySegmentTheManifestsName(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 4, 3, 2, 5)
	// Neither the files a backup keeps outside its streams nor a stray file in
	// a data directory is a segment.
	b.put(path.Join(testBackupID, "metadata.json"), []byte(`{"backup_id":"519118324"}`))
	b.put(path.Join(testBackupID, namespacesDir, testNS, string(QueryStream), dataDir, "p0", "state.bin"),
		[]byte("state"))

	want := b.recordedSegments()

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)

			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamAll(t.Context(), out)
			})

			if got := paths(segments); !slices.Equal(got, want) {
				t.Fatalf("streamed %d segments, want %d\n got: %v\nwant: %v",
					len(got), len(want), got, want)
			}

			stats := s.Stats()

			// The manifests are the only thing a full run downloads.
			if stats.ManifestsRead != tc.store.opened.Load() || stats.ManifestsRead != stats.ManifestsFound {
				t.Errorf("read %d of %d manifests and downloaded %d files, want every manifest and nothing else",
					stats.ManifestsRead, stats.ManifestsFound, tc.store.opened.Load())
			}

			if stats.Segments != int64(len(want)) || stats.Namespaces != 1 {
				t.Errorf("stats = %+v, want %d segments of one namespace", stats, len(want))
			}
		})
	}
}

func TestStreamAll_SegmentNoManifestNames(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 2, 2, 1, 2)
	// A segment the storage holds that no manifest names is read like the
	// others and marked, because nothing says what it should be.
	loose := path.Join(testBackupID, namespacesDir, testNS, string(QueryStream), dataDir, "loose.seg")
	b.put(loose, segmentBody(32))
	inPartition := path.Join(testBackupID, namespacesDir, testNS, string(QueryStream), dataDir, "p0", "extra.seg")
	b.put(inPartition, segmentBody(48))

	want := b.segments()

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)

			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamAll(t.Context(), out)
			})

			if got := paths(segments); !slices.Equal(got, want) {
				t.Fatalf("streamed %d segments, want the %d of the backup:\n got: %v\nwant: %v",
					len(got), len(want), got, want)
			}

			unrecorded := make([]string, 0, 2)

			for _, seg := range segments {
				if !seg.Unrecorded {
					continue
				}

				unrecorded = append(unrecorded, seg.Path)

				if seg.Manifest != "" || seg.Checksum != "" {
					t.Errorf("segment %q is unrecorded but carries manifest %q", seg.Path, seg.Manifest)
				}
			}

			sort.Strings(unrecorded)

			if !slices.Equal(unrecorded, []string{loose, inPartition}) {
				t.Fatalf("unrecorded = %v, want %v", unrecorded, []string{loose, inPartition})
			}

			if s.Stats().Unrecorded != 2 {
				t.Errorf("stats unrecorded = %d, want 2", s.Stats().Unrecorded)
			}
		})
	}
}

func TestStreamAll_BackupInOrderCostsNoSecondLook(t *testing.T) {
	t.Parallel()

	// A backup holding exactly what its manifests record is reconciled by
	// counting, so no manifest is read twice.
	b := newTestBackupTree(t, 4, 3, 1, 3)

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)

			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamAll(t.Context(), out)
			})

			if got := paths(segments); !slices.Equal(got, b.recordedSegments()) {
				t.Fatalf("streamed %d segments, want the %d the manifests record", len(got), len(b.recorded))
			}

			if opened, read := tc.store.opened.Load(), s.Stats().ManifestsRead; opened != read {
				t.Errorf("downloaded %d files for %d manifests, want each manifest read once", opened, read)
			}
		})
	}
}

func TestStreamAll_MissingSegmentIsStillNamed(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 2, 2, 0, 0)
	gone := b.recordedSegments()[0]
	b.remove(gone)

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)

			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamAll(t.Context(), out)
			})

			// Reading the manifests is what makes a lost segment visible: a
			// listing of the data would simply not mention it.
			if !slices.Contains(paths(segments), gone) {
				t.Fatalf("streamed %v, want the missing segment %q named", paths(segments), gone)
			}

			if _, err := s.OpenSegment(t.Context(), &Segment{Path: gone}); !errors.Is(err, ErrSegmentMissing) {
				t.Fatalf("OpenSegment() error = %v, want ErrSegmentMissing", err)
			}
		})
	}
}

func TestStreamAll_FallsBackToTheDataOfAStreamWithoutManifests(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 3, 2, 1, 2)

	for p := range 3 {
		b.remove(b.manifestPath(p))
	}

	// The query stream lost its manifests, so its data is read as it is found,
	// loose segments included. The change stream still has its own.
	loose := path.Join(testBackupID, namespacesDir, testNS, string(QueryStream), dataDir, "loose.seg")
	b.put(loose, segmentBody(32))

	want := b.segments()

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)

			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamAll(t.Context(), out)
			})

			if got := paths(segments); !slices.Equal(got, want) {
				t.Fatalf("streamed %d segments, want the %d of the backup:\n got: %v\nwant: %v",
					len(got), len(want), got, want)
			}

			for _, seg := range segments {
				recorded := seg.Stream == ChangeStream

				if (seg.Manifest != "") != recorded {
					t.Errorf("segment %q names manifest %q, want the listed stream to name none", seg.Path, seg.Manifest)
				}
			}
		})
	}
}

func TestStreamAll_UnusableManifestIsReported(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 2, 2, 0, 0)
	b.put(b.manifestPath(0), []byte(`{"segments": [ this is not a manifest`))

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)

			// One broken manifest does not stop the run: the partitions the
			// other manifests describe are still read.
			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamAll(t.Context(), out)
			})

			// The partition of the broken manifest is still read: its segments
			// are there, and nothing readable records them.
			if len(segments) != 4 {
				t.Fatalf("streamed %d segments past a broken manifest, want the 2 recorded and the 2 that are not",
					len(segments))
			}

			unrecorded := 0

			for _, seg := range segments {
				if seg.Unrecorded {
					unrecorded++
				}
			}

			if unrecorded != 2 {
				t.Errorf("streamed %d unrecorded segments, want the 2 of the broken manifest", unrecorded)
			}

			stats := s.Stats()

			if stats.ManifestsFailed != 1 || len(stats.ManifestIssues) != 1 {
				t.Fatalf("stats = %+v, want the one unusable manifest reported", stats)
			}

			if !errors.Is(stats.ManifestIssues[0].Err, ErrManifestUnusable) {
				t.Errorf(msgWantManifestUnusable, stats.ManifestIssues[0].Err)
			}
		})
	}
}

// A query stream manifest the server cannot parse the name of is one it skips
// on restore, so the segments it records are never restored: the manifest is
// reported, whatever it records, and no segment is taken from it.
func TestStreaming_ManifestTheServerSkipsIsReported(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// giveBody makes the body of the misnamed manifest out of the one the
		// backup was written with.
		giveBody func(written []byte) []byte
	}{
		{
			name:     "manifest recording segments",
			giveBody: func(written []byte) []byte { return written },
		},
		{
			name:     "manifest recording nothing",
			giveBody: func([]byte) []byte { return testManifest(testNS, 0, nil) },
		},
	}

	for _, tt := range tests {
		b := newTestBackupTree(t, 2, 2, 0, 0)
		manifest := b.manifestPath(0)
		skipped := path.Join(path.Dir(manifest), "0-7-0000181197010.json")
		b.put(skipped, tt.giveBody(b.files[manifest]))
		b.remove(manifest)

		for _, tc := range storesOf(t, b) {
			for name, run := range streamRuns(t.Context(), 4) {
				t.Run(path.Join(tt.name, tc.name, name), func(t *testing.T) {
					t.Parallel()

					s := newTestStreamer(t, tc.store)

					segments := collect(t, func(out chan<- Segment) error {
						return run(s, out)
					})

					for _, seg := range segments {
						if seg.Manifest == skipped {
							t.Fatalf("segment %q taken from the skipped manifest", seg.Path)
						}
					}

					stats := s.Stats()

					if len(stats.ManifestIssues) != 1 || stats.ManifestIssues[0].Path != skipped {
						t.Fatalf("stats = %+v, want the skipped manifest reported", stats)
					}

					if !errors.Is(stats.ManifestIssues[0].Err, ErrManifestUnusable) {
						t.Errorf(msgWantManifestUnusable, stats.ManifestIssues[0].Err)
					}
				})
			}
		}
	}
}

func TestStreaming_ManifestEntryTheServerRefusesIsReported(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// giveEdit breaks the first segment entry of the manifest.
		giveEdit func(entry map[string]any)
	}{
		{
			name:     "size left out",
			giveEdit: func(entry map[string]any) { delete(entry, manifestSizeKey) },
		},
		{
			name:     "checksum left out",
			giveEdit: func(entry map[string]any) { delete(entry, manifestChecksumKey) },
		},
		{
			name:     "size written as null",
			giveEdit: func(entry map[string]any) { entry[manifestSizeKey] = nil },
		},
		{
			name:     "checksum written as null",
			giveEdit: func(entry map[string]any) { entry[manifestChecksumKey] = nil },
		},
	}

	for _, tt := range tests {
		b := newTestBackupTree(t, 2, 2, 0, 0)
		manifest := b.manifestPath(0)
		b.put(manifest, editSegment(t, b.files[manifest], 0, tt.giveEdit))

		for _, tc := range storesOf(t, b) {
			for name, run := range streamRuns(t.Context(), 4) {
				t.Run(path.Join(tt.name, tc.name, name), func(t *testing.T) {
					t.Parallel()

					s := newTestStreamer(t, tc.store)

					segments := collect(t, func(out chan<- Segment) error {
						return run(s, out)
					})

					for _, seg := range segments {
						if seg.Manifest == manifest {
							t.Fatalf("segment %q taken from the refused manifest", seg.Path)
						}
					}

					stats := s.Stats()

					if len(stats.ManifestIssues) != 1 || stats.ManifestIssues[0].Path != manifest {
						t.Fatalf("stats = %+v, want the refused manifest reported", stats)
					}

					if !errors.Is(stats.ManifestIssues[0].Err, ErrManifestUnusable) {
						t.Errorf(msgWantManifestUnusable, stats.ManifestIssues[0].Err)
					}
				})
			}
		}
	}
}

func TestStreamSample_MalformedEntryOutsideTheSampleIsReported(t *testing.T) {
	t.Parallel()

	const segments = 8

	// A sample is seeded, and which entries it picks does not depend on what
	// they hold, so a run over a sound backup tells which entries a run over
	// the same backup with one entry broken leaves out.
	sound := newTestBackupTree(t, 1, segments, 0, 0)
	manifest := sound.manifestPath(0)

	for i, tc := range storesOf(t, sound) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			picked := collect(t, func(out chan<- Segment) error {
				return newTestStreamer(t, tc.store).StreamSample(t.Context(), 1, out)
			})
			if len(picked) != 1 {
				t.Fatalf("sample = %+v, want one segment", picked)
			}

			// The manifest records the segments in the order they were written.
			left := slices.IndexFunc(sound.recorded, func(p string) bool {
				return p != picked[0].Path
			})

			broken := newTestBackupTree(t, 1, segments, 0, 0)
			broken.put(manifest, editSegment(t, broken.files[manifest], left,
				func(entry map[string]any) { delete(entry, manifestChecksumKey) }))

			s := newTestStreamer(t, storesOf(t, broken)[i].store)
			collect(t, func(out chan<- Segment) error {
				return s.StreamSample(t.Context(), 1, out)
			})

			stats := s.Stats()

			if len(stats.ManifestIssues) != 1 || stats.ManifestIssues[0].Path != manifest {
				t.Fatalf("stats = %+v, want the manifest with a broken entry reported", stats)
			}

			if !errors.Is(stats.ManifestIssues[0].Err, ErrManifestUnusable) {
				t.Errorf(msgWantManifestUnusable, stats.ManifestIssues[0].Err)
			}
		})
	}
}

// editSegment rewrites a manifest with edit applied to the segment entry it
// records at index.
func editSegment(
	t *testing.T, manifest []byte, index int, edit func(entry map[string]any),
) []byte {
	t.Helper()

	var m map[string]any
	if err := json.Unmarshal(manifest, &m); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	segments, ok := m[manifestSegmentsKey].([]any)
	if !ok || index < 0 || index >= len(segments) {
		t.Fatalf("manifest records no segment %d: %s", index, manifest)
	}

	entry, ok := segments[index].(map[string]any)
	if !ok {
		t.Fatalf("segment entry = %T, want an object", segments[index])
	}

	edit(entry)

	body, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	return body
}

func TestStreamAll_DataDirectoryOfItsOwn(t *testing.T) {
	t.Parallel()

	// A data directory holding more segments than a level is worth walking is
	// walked as a whole instead, which is what a stream writing millions of
	// them into one directory looks like.
	b := newTestBackup(testBackupID)
	root := path.Join(testBackupID, namespacesDir, testNS, string(QueryStream), dataDir)

	for i := range maxLooseSegments + 100 {
		b.put(path.Join(root, fmt.Sprintf("7-%010d.seg", i)), segmentBody(8))
	}

	// One partition below the same directory must not be walked twice.
	b.put(path.Join(root, "p1", "7-0000000001.seg"), segmentBody(8))

	bucket := newFakeBucket(b.files)
	bucket.pageSize = 1000

	s := newTestStreamer(t, &s3Store{client: bucket, bucket: testBucket})

	segments := collect(t, func(out chan<- Segment) error {
		return s.StreamAll(t.Context(), out)
	})

	if got := paths(segments); !slices.Equal(got, b.segments()) {
		t.Fatalf("streamed %d segments, want the %d of the backup", len(got), len(b.segments()))
	}
}

func TestStreamAll_DescribesEverySegment(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 1, 1, 1, 1)

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)

			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamAll(t.Context(), out)
			})

			byStream := make(map[Stream]Segment, len(segments))

			for _, seg := range segments {
				byStream[seg.Stream] = seg
			}

			if len(byStream) != 2 {
				t.Fatalf("streams = %v, want both streams of the namespace", byStream)
			}

			for stream, seg := range byStream {
				if seg.Namespace != testNS {
					t.Errorf("%s namespace = %q, want %q", stream, seg.Namespace, testNS)
				}

				if seg.Size != int64(len(b.files[seg.Path])) {
					t.Errorf("%s size = %d, want %d", stream, seg.Size, len(b.files[seg.Path]))
				}

				if _, ok := b.files[seg.Manifest]; !ok {
					t.Errorf("%s manifest = %q, which the backup does not hold", stream, seg.Manifest)
				}

				if seg.Checksum != testChecksum {
					t.Errorf("%s checksum = %q, want %q", stream, seg.Checksum, testChecksum)
				}

				// Only a query stream manifest records how many records a
				// segment holds.
				wantCount := int64(0)
				if stream == QueryStream {
					wantCount = testRecordCount
				}

				if seg.RecordCount != wantCount {
					t.Errorf("%s record count = %d, want %d", stream, seg.RecordCount, wantCount)
				}
			}
		})
	}
}

func TestStreamAll_UnknownBackupYieldsNothing(t *testing.T) {
	t.Parallel()

	for _, tc := range storesOf(t, newTestBackupTree(t, 1, 1, 0, 0)) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s, err := newStreamer(tc.store, "no-such-backup")
			if err != nil {
				t.Fatalf("newStreamer() error = %v", err)
			}

			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamAll(t.Context(), out)
			})

			if len(segments) != 0 {
				t.Fatalf("streamed %d segments of a backup that does not exist", len(segments))
			}
		})
	}
}

func TestStreamSample_SizeIsHonoured(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 50, 4, 2, 6)

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			for _, n := range []int{1, 3, 17, 60} {
				s := newTestStreamer(t, tc.store)

				segments := collect(t, func(out chan<- Segment) error {
					return s.StreamSample(t.Context(), n, out)
				})

				if len(segments) != n {
					t.Errorf("sample of %d holds %d segments", n, len(segments))
				}

				if len(unique(paths(segments))) != len(segments) {
					t.Errorf("sample of %d holds the same segment twice: %v", n, paths(segments))
				}

				for _, seg := range segments {
					if _, ok := b.files[seg.Path]; !ok {
						t.Errorf("sampled %q, which the backup does not hold", seg.Path)
					}
				}
			}
		})
	}
}

func TestStreamSample_ComesFromTheManifests(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 20, 2, 1, 4)

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			const sampleSize = 12

			s := newTestStreamer(t, tc.store)

			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamSample(t.Context(), sampleSize, out)
			})

			for _, seg := range segments {
				if seg.Manifest == "" {
					t.Fatalf("segment %q names no manifest, want the sample drawn from the manifests", seg.Path)
				}

				if _, ok := b.files[seg.Manifest]; !ok {
					t.Errorf("segment %q names manifest %q, which the backup does not hold", seg.Path, seg.Manifest)
				}

				// The size and the checksum come from the manifest, which is
				// what makes them worth comparing against the storage.
				if seg.Size != int64(len(b.files[seg.Path])) {
					t.Errorf("segment %q recorded size = %d, want %d", seg.Path, seg.Size, len(b.files[seg.Path]))
				}

				if seg.Checksum != testChecksum {
					t.Errorf("segment %q checksum = %q, want %q", seg.Path, seg.Checksum, testChecksum)
				}
			}

			// Only the manifests are downloaded to pick a sample; the segments
			// are downloaded by whoever validates them.
			stats := s.Stats()

			if stats.ManifestsRead != tc.store.opened.Load() {
				t.Errorf("read %d manifests but downloaded %d files, want only the manifests",
					stats.ManifestsRead, tc.store.opened.Load())
			}

			if stats.ManifestsFound != 21 || stats.ManifestsFailed != 0 {
				t.Errorf("stats = %+v, want the 21 manifests of the backup, none of them failing", stats)
			}
		})
	}
}

func TestStreamSample_TinySampleGoesWhereTheDataIs(t *testing.T) {
	t.Parallel()

	// The change stream is three units against the one of the query stream, so
	// a sample of one must still land where the segments are.
	b := newTestBackupTree(t, 100, 2, 3, 2)

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)

			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamSample(t.Context(), 1, out)
			})

			if len(segments) != 1 || segments[0].Stream != QueryStream {
				t.Fatalf("sample = %+v, want one segment of the query stream", segments)
			}
		})
	}
}

func TestStreamSample_ChecksumIsCarriedWhateverTheDeclaredAlgorithm(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 1, 2, 0, 0)
	// The server checks every segment against CRC-32C whatever algorithm its
	// manifest declares, so the declaration changes nothing.
	manifest := b.files[b.manifestPath(0)]
	b.put(b.manifestPath(0),
		bytes.ReplaceAll(manifest,
			[]byte(`"checksum_algorithm":"crc32c"`), []byte(`"checksum_algorithm":"xxh3"`)))

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)

			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamSample(t.Context(), 2, out)
			})

			for _, seg := range segments {
				if seg.Manifest == "" || seg.Checksum != testChecksum {
					t.Errorf("segment %q carries checksum %q, want %q",
						seg.Path, seg.Checksum, testChecksum)
				}
			}
		})
	}
}

func TestStreamSample_CoversBothStreams(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 100, 2, 2, 4)

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)

			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamSample(t.Context(), 40, out)
			})

			byStream := make(map[Stream]int)

			for _, seg := range segments {
				byStream[seg.Stream]++
			}

			if byStream[ChangeStream] < 2 || byStream[QueryStream] < 2 {
				t.Fatalf("sample by stream = %v, want both streams of the backup covered", byStream)
			}

			// Most of the data is in the query stream, and so is most of the
			// sample.
			if byStream[QueryStream] <= byStream[ChangeStream] {
				t.Errorf("sample by stream = %v, want the wider stream to carry the sample", byStream)
			}
		})
	}
}

func TestStreamSample_IsRepeatable(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 40, 3, 1, 4)

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sample := func(seed uint64) []string {
				s := newTestStreamer(t, tc.store, WithSeed(seed))

				return paths(collect(t, func(out chan<- Segment) error {
					return s.StreamSample(t.Context(), 20, out)
				}))
			}

			first, again := sample(7), sample(7)
			if !slices.Equal(first, again) {
				t.Fatalf("the same seed sampled differently:\n%v\n%v", first, again)
			}

			if other := sample(99); slices.Equal(first, other) {
				t.Errorf("another seed sampled the same segments: %v", other)
			}
		})
	}
}

func TestStreamSample_SmallBackupIsSampledWhole(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 3, 2, 1, 2)
	want := b.segments()

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)

			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamSample(t.Context(), len(want)*4, out)
			})

			if got := unique(paths(segments)); !slices.Equal(got, want) {
				t.Fatalf("sampled %v, want every segment of the backup %v", got, want)
			}
		})
	}
}

func TestStreamSample_NothingIsAskedFor(t *testing.T) {
	t.Parallel()

	for _, tc := range storesOf(t, newTestBackupTree(t, 2, 2, 1, 2)) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)

			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamSample(t.Context(), 0, out)
			})

			if len(segments) != 0 || tc.store.opened.Load() != 0 {
				t.Fatalf("a sample of nothing streamed %d segments and downloaded %d files",
					len(segments), tc.store.opened.Load())
			}
		})
	}
}

// TestStreaming_StreamWithoutADataDirectory covers the two backups whose
// streams hold manifests and no data: one of a namespace that held no records,
// which recorded nothing, and one whose segments are gone, whose manifests
// still promise them. A stream is not passed over for having no data
// directory, because its manifests are the only place a segment that is no
// longer there can still be found.
func TestStreaming_StreamWithoutADataDirectory(t *testing.T) {
	t.Parallel()

	empty := newTestBackupTree(t, 0, 0, 0, 0)
	for p := range 3 {
		empty.queryPartition(testNS, p, 0)
	}

	lost := newTestBackupTree(t, 3, 2, 0, 0)
	for _, seg := range lost.segments() {
		lost.remove(seg)
	}

	// A change stream keeps a directory per node, so the same stream with no
	// data hangs one level deeper.
	lostNode := newTestBackupTree(t, 0, 0, 1, 2)
	for _, seg := range lostNode.segments() {
		lostNode.remove(seg)
	}

	tests := []struct {
		backup    *testBackup
		name      string
		want      []string
		manifests int64
	}{
		{name: "recorded nothing", backup: empty, want: nil, manifests: 3},
		{name: "data is gone", backup: lost, want: lost.recordedSegments(), manifests: 3},
		{name: "data of a node is gone", backup: lostNode, want: lostNode.recordedSegments(), manifests: 1},
	}

	for _, tt := range tests {
		for _, tc := range storesOf(t, tt.backup) {
			t.Run(tt.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()

				s := newTestStreamer(t, tc.store)

				segments := collect(t, func(out chan<- Segment) error {
					return s.StreamAll(t.Context(), out)
				})

				if got := paths(segments); !slices.Equal(got, tt.want) {
					t.Fatalf("streamed %v, want the segments the manifests name %v", got, tt.want)
				}

				// The manifests were read, whether or not they led anywhere,
				// which is what tells a backup holding nothing from a backup
				// id nothing was written under.
				if stats := s.Stats(); stats.ManifestsRead != tt.manifests || stats.Namespaces != 1 {
					t.Errorf("stats = %+v, want the %d manifests of the one namespace read",
						stats, tt.manifests)
				}
			})
		}
	}
}

func TestStreamSample_BackupHoldingNothing(t *testing.T) {
	t.Parallel()

	for _, tc := range storesOf(t, newTestBackupTree(t, 0, 0, 0, 0)) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)

			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamSample(t.Context(), 10_000, out)
			})

			if len(segments) != 0 {
				t.Fatalf("a backup holding nothing sampled %d segments", len(segments))
			}
		})
	}
}

func TestStreamSample_FallsBackToTheDataOfAStreamWithoutManifests(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 6, 3, 0, 0)

	for p := range 6 {
		b.remove(b.manifestPath(p))
	}

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)

			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamSample(t.Context(), 9, out)
			})

			if len(segments) != 9 {
				t.Fatalf("sampled %d segments of a stream without manifests, want 9", len(segments))
			}

			for _, seg := range segments {
				if seg.Manifest != "" {
					t.Errorf("segment %q names manifest %q, want none", seg.Path, seg.Manifest)
				}

				if _, ok := b.files[seg.Path]; !ok {
					t.Errorf("sampled %q, which the backup does not hold", seg.Path)
				}
			}

			if tc.store.opened.Load() != 0 {
				t.Errorf("downloaded %d files, want a listing to download nothing", tc.store.opened.Load())
			}
		})
	}
}

func TestStreamSample_UnusableManifestIsReported(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 1, 2, 0, 0)
	b.put(b.manifestPath(0), []byte(`{"segments": [ this is not a manifest`))

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)

			// A manifest that cannot be read is a finding, not a failure, and
			// the segments below it are still checked.
			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamSample(t.Context(), 2, out)
			})

			if len(segments) != 2 {
				t.Fatalf("sampled %d segments past an unusable manifest, want 2", len(segments))
			}

			stats := s.Stats()

			if stats.ManifestsFailed != 1 || len(stats.ManifestIssues) != 1 {
				t.Fatalf("stats = %+v, want the one unusable manifest to be reported", stats)
			}

			issue := stats.ManifestIssues[0]

			if issue.Path != b.manifestPath(0) || issue.Namespace != testNS {
				t.Errorf("issue = %+v, want the manifest of partition 0", issue)
			}

			if !errors.Is(issue.Err, ErrManifestUnusable) {
				t.Errorf(msgWantManifestUnusable, issue.Err)
			}
		})
	}
}

// TestStreaming_EmptyManifestIsReported covers the manifest an interrupted
// flush leaves behind: a zero sized object where a manifest should be. Reading
// one fails on the very first token, which is the failure furthest from
// anything a manifest says, and it still has to be a finding about the backup
// rather than the end of the run.
func TestStreaming_EmptyManifestIsReported(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 2, 2, 0, 0)
	b.put(b.manifestPath(0), nil)

	for _, tc := range storesOf(t, b) {
		for name, run := range streamRuns(t.Context(), 4) {
			t.Run(path.Join(tc.name, name), func(t *testing.T) {
				t.Parallel()

				s := newTestStreamer(t, tc.store)

				segments := collect(t, func(out chan<- Segment) error {
					return run(s, out)
				})

				if len(segments) == 0 {
					t.Fatal("an empty manifest left the run with nothing, want the rest of the backup")
				}

				stats := s.Stats()

				if stats.ManifestsFailed != 1 || len(stats.ManifestIssues) != 1 {
					t.Fatalf("stats = %+v, want the empty manifest to be reported", stats)
				}

				if !errors.Is(stats.ManifestIssues[0].Err, ErrManifestUnusable) {
					t.Errorf(msgWantManifestUnusable, stats.ManifestIssues[0].Err)
				}
			})
		}
	}
}

func TestStreamSample_ManifestNamingAMissingSegment(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 1, 2, 0, 0)
	gone := b.segments()[0]
	b.remove(gone)

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)

			// The listing no longer holds the segment, but the manifest still
			// names it, which is the whole point of sampling from manifests.
			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamSample(t.Context(), 2, out)
			})

			if !slices.Contains(paths(segments), gone) {
				t.Fatalf("sample = %v, want it to name the missing segment %q", paths(segments), gone)
			}

			_, err := s.OpenSegment(t.Context(), &Segment{Path: gone})
			if !errors.Is(err, ErrSegmentMissing) {
				t.Fatalf("OpenSegment() error = %v, want ErrSegmentMissing", err)
			}
		})
	}
}

func TestOpenSegment(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 1, 1, 0, 0)
	want := b.segments()[0]

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)

			body, err := s.OpenSegment(t.Context(), &Segment{Path: want})
			if err != nil {
				t.Fatalf("OpenSegment() error = %v", err)
			}
			defer body.Close()

			got, err := io.ReadAll(body)
			if err != nil {
				t.Fatalf("read segment: %v", err)
			}

			if !bytes.Equal(got, b.files[want]) {
				t.Errorf("read %d bytes, want the %d bytes of the segment", len(got), len(b.files[want]))
			}
		})
	}
}

func TestStreaming_StopsOnACanceledContext(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 20, 5, 1, 5)

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			s := newTestStreamer(t, tc.store)

			out := make(chan Segment, 1)
			if err := s.StreamAll(ctx, out); !errors.Is(err, context.Canceled) {
				t.Errorf("StreamAll() error = %v, want context.Canceled", err)
			}

			out = make(chan Segment, 1)
			if err := s.StreamSample(ctx, 10, out); !errors.Is(err, context.Canceled) {
				t.Errorf("StreamSample() error = %v, want context.Canceled", err)
			}
		})
	}
}

func TestStreaming_ThrottlesOnASlowConsumer(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 8, 4, 1, 4)

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)

			// An unbuffered channel read one segment at a time proves that the
			// run does not need a consumer keeping up with it.
			out := make(chan Segment)
			done := make(chan error, 1)

			go func() { done <- s.StreamAll(t.Context(), out) }()

			count := 0
			for range out {
				count++
			}

			if err := <-done; err != nil {
				t.Fatalf("StreamAll() error = %v", err)
			}

			if count != len(b.segments()) {
				t.Errorf("streamed %d segments, want %d", count, len(b.segments()))
			}
		})
	}
}

// unique removes the repeated values of a sorted slice.
func unique[T comparable](values []T) []T {
	found := make([]T, 0, len(values))

	for i, v := range values {
		if i == 0 || values[i-1] != v {
			found = append(found, v)
		}
	}

	return found
}
