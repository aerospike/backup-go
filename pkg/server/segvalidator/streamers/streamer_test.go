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
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"strings"
	"sync/atomic"
	"testing"
)

// errStorage is what a storage that stopped answering reports.
var errStorage = errors.New("storage is unreachable")

// faultyStore makes a storage fail one of the operations a run makes of it,
// which is how a test proves that a storage that stops answering ends the run
// instead of quietly shortening it.
type faultyStore struct {
	store
	failLevel func(dir string) error
	failFiles func(dir string) error
	failOpen  func(storagePath string) error
}

func (f *faultyStore) listLevel(ctx context.Context, dir string, fn func(levelEntry) error) error {
	if f.failLevel != nil {
		if err := f.failLevel(dir); err != nil {
			return err
		}
	}

	return f.store.listLevel(ctx, dir, fn)
}

func (f *faultyStore) listFiles(ctx context.Context, dir string, fn func(file) error) error {
	if f.failFiles != nil {
		if err := f.failFiles(dir); err != nil {
			return err
		}
	}

	return f.store.listFiles(ctx, dir, fn)
}

func (f *faultyStore) open(ctx context.Context, storagePath string) (io.ReadCloser, error) {
	if f.failOpen != nil {
		if err := f.failOpen(storagePath); err != nil {
			return nil, err
		}
	}

	return f.store.open(ctx, storagePath)
}

// failingOn fails every operation whose path contains match.
func failingOn(match string) func(string) error {
	return func(p string) error {
		if strings.Contains(p, match) {
			return errStorage
		}

		return nil
	}
}

// newFaultyStreamer creates a streamer over a local copy of a backup whose
// storage the caller has made fail.
func newFaultyStreamer(t *testing.T, b *testBackup, fault func(*faultyStore)) *Streamer {
	t.Helper()

	root := t.TempDir()
	writeBackup(t, root, b)

	st := &faultyStore{store: &localStore{root: root}}
	fault(st)

	return newTestStreamer(t, st)
}

// drain runs a streaming call and returns what it failed with, reading the
// segments it produced so that it is never blocked on the channel.
func drain(run func(chan<- Segment) error) error {
	out := make(chan Segment, 16)
	done := make(chan error, 1)

	go func() { done <- run(out) }()

	for range out { //nolint:revive // The segments are drained, not inspected.
	}

	return <-done
}

func TestStreamerOptions(t *testing.T) {
	t.Parallel()

	const (
		concurrency = 3
		scanLimit   = 7
		seed        = uint64(0x5eed)
	)

	logger := slog.New(slog.DiscardHandler)

	s, err := newStreamer(&localStore{root: t.TempDir()}, testBackupID,
		WithLogger(logger), WithConcurrency(concurrency), WithScanLimit(scanLimit), WithSeed(seed))
	if err != nil {
		t.Fatalf("newStreamer() error = %v", err)
	}

	if s.logger != logger {
		t.Error("WithLogger() did not set the logger")
	}

	if s.options.concurrency != concurrency {
		t.Errorf("concurrency = %d, want %d", s.options.concurrency, concurrency)
	}

	if s.options.scanLimit != scanLimit {
		t.Errorf("scanLimit = %d, want %d", s.options.scanLimit, scanLimit)
	}

	if s.options.seed != seed {
		t.Errorf("seed = %d, want %d", s.options.seed, seed)
	}
}

func TestStreamerOptions_OutOfRangeAreIgnored(t *testing.T) {
	t.Parallel()

	s, err := newStreamer(&localStore{root: t.TempDir()}, testBackupID,
		WithLogger(nil), WithConcurrency(0), WithScanLimit(-1))
	if err != nil {
		t.Fatalf("newStreamer() error = %v", err)
	}

	if s.logger == nil {
		t.Error("WithLogger(nil) cleared the logger, want the default kept")
	}

	if s.options.concurrency != defaultConcurrency || s.options.scanLimit != defaultScanLimit {
		t.Errorf("options = %+v, want the defaults kept", s.options)
	}
}

func TestStopped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err     error
		wantErr error
		name    string
	}{
		{name: "nothing went wrong", err: nil, wantErr: nil},
		{name: "the caller has seen enough", err: errStopListing, wantErr: nil},
		{
			name:    "the caller has seen enough, wrapped",
			err:     fmt.Errorf("partition: %w", errStopListing),
			wantErr: nil,
		},
		{name: "the listing failed", err: errStorage, wantErr: errStorage},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := stopped(tt.err)

			if tt.wantErr == nil {
				if got != nil {
					t.Fatalf("stopped() = %v, want nil", got)
				}

				return
			}

			if !errors.Is(got, tt.wantErr) {
				t.Fatalf("stopped() = %v, want %v", got, tt.wantErr)
			}
		})
	}
}

// The listings that discover the shape of a backup come before anything else,
// and a storage that will not answer them says nothing about the backup, so a
// run fails rather than reporting an empty one.
func TestStreaming_ShapeOfTheBackupCannotBeListed(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 2, 2, 1, 2)

	tests := []struct {
		name string
		dir  string
	}{
		{name: "the namespaces", dir: path.Join(testBackupID, namespacesDir)},
		{
			name: "the streams of a namespace",
			dir:  path.Join(testBackupID, namespacesDir, testNS, string(QueryStream)),
		},
		{
			name: "the data of a unit",
			dir:  path.Join(testBackupID, namespacesDir, testNS, string(QueryStream), dataDir),
		},
		{
			name: "the nodes of a change stream",
			dir: path.Join(testBackupID, namespacesDir, testNS, string(ChangeStream),
				fmt.Sprintf("BB951D8A16DC7%02X", 0)),
		},
		{
			name: "the data of a node of a change stream",
			dir: path.Join(testBackupID, namespacesDir, testNS, string(ChangeStream),
				fmt.Sprintf("BB951D8A16DC7%02X", 0), dataDir),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for _, name := range []string{"all", "sample"} {
				t.Run(name, func(t *testing.T) {
					t.Parallel()

					s := newFaultyStreamer(t, b, func(st *faultyStore) {
						st.failLevel = func(dir string) error {
							if dir == tt.dir {
								return errStorage
							}

							return nil
						}
					})

					err := drain(func(out chan<- Segment) error {
						if name == "all" {
							return s.StreamAll(t.Context(), out)
						}

						return s.StreamSample(t.Context(), 4, out)
					})

					if !errors.Is(err, errStorage) {
						t.Fatalf("streaming error = %v, want the storage failure", err)
					}
				})
			}
		})
	}
}

func TestStreamAll_ListingTheManifestsFails(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 2, 2, 0, 0)

	s := newFaultyStreamer(t, b, func(st *faultyStore) {
		st.failFiles = failingOn(manifestDir)
	})

	err := drain(func(out chan<- Segment) error { return s.StreamAll(t.Context(), out) })
	if !errors.Is(err, errStorage) {
		t.Fatalf("StreamAll() error = %v, want the storage failure", err)
	}
}

func TestStreamSample_ListingTheManifestsFails(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 2, 2, 0, 0)

	s := newFaultyStreamer(t, b, func(st *faultyStore) {
		st.failFiles = failingOn(manifestDir)
	})

	err := drain(func(out chan<- Segment) error { return s.StreamSample(t.Context(), 4, out) })
	if !errors.Is(err, errStorage) {
		t.Fatalf("StreamSample() error = %v, want the storage failure", err)
	}
}

// A manifest that cannot be parsed is reported and stepped over; a storage
// that will not hand one over at all is a different thing, and ends the run.
func TestStreaming_DownloadingAManifestFails(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 2, 2, 0, 0)

	for _, name := range []string{"all", "sample"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s := newFaultyStreamer(t, b, func(st *faultyStore) {
				st.failOpen = failingOn(manifestDir)
			})

			err := drain(func(out chan<- Segment) error {
				if name == "all" {
					return s.StreamAll(t.Context(), out)
				}

				return s.StreamSample(t.Context(), 4, out)
			})

			if !errors.Is(err, errStorage) {
				t.Fatalf("streaming error = %v, want the storage failure", err)
			}
		})
	}
}

// A stream whose manifests are gone is read from its data directories, and a
// storage that will not list those has nothing left to offer.
func TestStreaming_ListingAPartitionOfAStreamWithoutManifestsFails(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 2, 2, 0, 0)
	for p := range 2 {
		b.remove(b.manifestPath(p))
	}

	for _, name := range []string{"all", "sample"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s := newFaultyStreamer(t, b, func(st *faultyStore) {
				st.failFiles = failingOn(dataDir)
			})

			err := drain(func(out chan<- Segment) error {
				if name == "all" {
					return s.StreamAll(t.Context(), out)
				}

				return s.StreamSample(t.Context(), 4, out)
			})

			if !errors.Is(err, errStorage) {
				t.Fatalf("streaming error = %v, want the storage failure", err)
			}
		})
	}
}

func TestStreamAll_CountingASegmentDirectoryFails(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 2, 2, 0, 0)

	// The manifests are read, and the data directories are then listed to find
	// what they hold beyond them.
	s := newFaultyStreamer(t, b, func(st *faultyStore) {
		st.failFiles = failingOn(path.Join(dataDir, "p1"))
	})

	err := drain(func(out chan<- Segment) error { return s.StreamAll(t.Context(), out) })
	if !errors.Is(err, errStorage) {
		t.Fatalf("StreamAll() error = %v, want the storage failure", err)
	}
}

// A directory holding more segments than its manifests recorded is reconciled
// by reading those manifests again. When that read fails, nothing can be said
// about the directory, and saying nothing is better than calling every segment
// in it unrecorded.
func TestStreamAll_ManifestThatCannotBeReadAgain(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 1, 2, 0, 0)
	extra := path.Join(testBackupID, namespacesDir, testNS, string(QueryStream), dataDir, "p0", "extra.seg")
	b.put(extra, segmentBody(32))

	var opened atomic.Int64

	s := newFaultyStreamer(t, b, func(st *faultyStore) {
		st.failOpen = func(storagePath string) error {
			// The first read is the one that streams the manifest; the second
			// is the one that reconciles the directory it recorded.
			if isManifest(storagePath) && opened.Add(1) > 1 {
				return errStorage
			}

			return nil
		}
	})

	segments := collect(t, func(out chan<- Segment) error { return s.StreamAll(t.Context(), out) })

	for _, seg := range segments {
		if seg.Unrecorded {
			t.Errorf("segment %q was called unrecorded, want a directory whose manifests "+
				"cannot be read again to be left alone", seg.Path)
		}
	}

	if s.Stats().Unrecorded != 0 {
		t.Errorf("stats unrecorded = %d, want none", s.Stats().Unrecorded)
	}
}

// Only the files a backup is made of are streamed. A directory of a backup can
// hold anything else, and none of it must reach a validator as backup data.
func TestStreaming_StrayFilesAreNotStreamed(t *testing.T) {
	t.Parallel()

	root := path.Join(testBackupID, namespacesDir, testNS, string(QueryStream))

	b := newTestBackupTree(t, 2, 2, 0, 0)
	// A stray file next to the manifests, and one next to the segments of a
	// stream whose manifests are gone.
	b.put(path.Join(root, manifestDir, "notes.txt"), []byte("not a manifest"))
	b.put(path.Join(root, dataDir, "p0", "state.bin"), []byte("not a segment"))
	b.put(path.Join(root, dataDir, "state.bin"), []byte("not a segment"))

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			for _, name := range []string{"all", "sample"} {
				t.Run(name, func(t *testing.T) {
					t.Parallel()

					s := newTestStreamer(t, tc.store)

					segments := collect(t, func(out chan<- Segment) error {
						if name == "all" {
							return s.StreamAll(t.Context(), out)
						}

						return s.StreamSample(t.Context(), 8, out)
					})

					for _, seg := range segments {
						if !isSegment(seg.Path) {
							t.Errorf("streamed %q, want segments only", seg.Path)
						}
					}

					if s.Stats().ManifestsFound != 2 {
						t.Errorf("found %d manifests, want the 2 of the backup", s.Stats().ManifestsFound)
					}
				})
			}
		})
	}
}

func TestStreamSample_StrayFilesOfAStreamWithoutManifests(t *testing.T) {
	t.Parallel()

	root := path.Join(testBackupID, namespacesDir, testNS, string(QueryStream))

	b := newTestBackupTree(t, 2, 2, 0, 0)
	for p := range 2 {
		b.remove(b.manifestPath(p))
	}

	b.put(path.Join(root, dataDir, "p0", "state.bin"), []byte("not a segment"))
	b.put(path.Join(root, dataDir, "p1", "state.bin"), []byte("not a segment"))

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			for _, name := range []string{"all", "sample"} {
				t.Run(name, func(t *testing.T) {
					t.Parallel()

					s := newTestStreamer(t, tc.store)

					segments := collect(t, func(out chan<- Segment) error {
						if name == "all" {
							return s.StreamAll(t.Context(), out)
						}

						return s.StreamSample(t.Context(), 8, out)
					})

					if len(segments) == 0 {
						t.Fatal("streamed nothing, want the segments of the data directories")
					}

					for _, seg := range segments {
						if !isSegment(seg.Path) {
							t.Errorf("streamed %q, want segments only", seg.Path)
						}
					}
				})
			}
		})
	}
}

// A listing is what bounds the work of sampling a directory of any size: past
// the scan limit a sample is drawn from what was scanned, and the rest of the
// directory is never looked at.
func TestStreamSample_ScanLimitBoundsTheListing(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 4, 4, 0, 0)

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store, WithScanLimit(1))

			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamSample(t.Context(), 4, out)
			})

			if len(segments) == 0 {
				t.Fatal("StreamSample() streamed nothing, want a sample of what it scanned")
			}

			// One manifest per listing was scanned, so no more than one
			// manifest per unit was read.
			if read := s.Stats().ManifestsRead; read > 1 {
				t.Errorf("read %d manifests, want at most the one the scan limit allowed", read)
			}
		})
	}
}

func TestStreamSample_ScanLimitBoundsAStreamWithoutManifests(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 2, 8, 0, 0)
	for p := range 2 {
		b.remove(b.manifestPath(p))
	}

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store, WithScanLimit(2))

			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamSample(t.Context(), 4, out)
			})

			if len(segments) == 0 {
				t.Fatal("StreamSample() streamed nothing, want a sample of what it scanned")
			}

			for _, seg := range segments {
				if !isSegment(seg.Path) {
					t.Errorf("streamed %q, want segments only", seg.Path)
				}
			}
		})
	}
}

// A stream directory holds the directories of its nodes, and it may hold other
// things too. Neither its own manifest directory nor a directory that is not a
// unit is mistaken for a node.
func TestStreamAll_StreamDirectoryHoldingMoreThanItsNodes(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 1, 2, 1, 2)
	root := path.Join(testBackupID, namespacesDir, testNS, string(ChangeStream))

	b.put(path.Join(root, manifestDir, "stray-0000181275309.json"), []byte("{}"))
	b.put(path.Join(root, "tmp", "leftover.txt"), []byte("not a unit"))

	want := b.recordedSegments()

	for _, tc := range storesOf(t, b) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStreamer(t, tc.store)

			segments := collect(t, func(out chan<- Segment) error {
				return s.StreamAll(t.Context(), out)
			})

			if got := paths(segments); len(got) != len(want) {
				t.Fatalf("streamed %d segments, want the %d of the backup:\n got: %v\nwant: %v",
					len(got), len(want), got, want)
			}
		})
	}
}

// A manifest naming a segment outside the storage is unusable, whichever way
// the backup is walked.
func TestStreaming_ManifestNamingASegmentOutsideTheStorage(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 1, 2, 0, 0)
	b.put(b.manifestPath(0), testManifest(testNS, 0, []manifestSegment{
		{SegmentName: "../../elsewhere/evil.seg", Size: 64},
	}))

	for _, name := range []string{"all", "sample"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			writeBackup(t, root, b)
			s := newTestStreamer(t, &localStore{root: root})

			segments := collect(t, func(out chan<- Segment) error {
				if name == "all" {
					return s.StreamAll(t.Context(), out)
				}

				return s.StreamSample(t.Context(), 4, out)
			})

			for _, seg := range segments {
				if strings.Contains(seg.Path, "elsewhere") {
					t.Errorf("streamed %q, want a segment outside the storage to be refused", seg.Path)
				}
			}

			stats := s.Stats()

			if stats.ManifestsFailed != 1 || len(stats.ManifestIssues) != 1 {
				t.Fatalf("stats = %+v, want the manifest reported as unusable", stats)
			}

			if !errors.Is(stats.ManifestIssues[0].Err, ErrManifestUnusable) {
				t.Errorf("manifest issue = %v, want ErrManifestUnusable", stats.ManifestIssues[0].Err)
			}
		})
	}
}

// A directory that turned out to hold more than its manifests recorded is
// listed a second time to name what is in it, and a storage that stops
// answering in between ends the run.
func TestStreamAll_SecondListingOfADirectoryFails(t *testing.T) {
	t.Parallel()

	b := newTestBackupTree(t, 1, 2, 0, 0)
	dir := path.Join(testBackupID, namespacesDir, testNS, string(QueryStream), dataDir, "p0")
	b.put(path.Join(dir, "extra.seg"), segmentBody(32))

	var listed atomic.Int64

	s := newFaultyStreamer(t, b, func(st *faultyStore) {
		st.failFiles = func(d string) error {
			// The first listing counts what the directory holds; the second
			// names the segments no manifest recorded in it.
			if d == dir && listed.Add(1) > 1 {
				return errStorage
			}

			return nil
		}
	})

	err := drain(func(out chan<- Segment) error { return s.StreamAll(t.Context(), out) })
	if !errors.Is(err, errStorage) {
		t.Fatalf("StreamAll() error = %v, want the storage failure", err)
	}
}

// A canceled run stops where it is instead of blocking on a consumer that
// stopped reading, whichever kind of segment it was about to hand over.
func TestStreaming_SendingIntoACanceledRun(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeBackup(t, root, newTestBackupTree(t, 1, 1, 0, 0))
	s := newTestStreamer(t, &localStore{root: root})

	u := unit{
		namespace: testNS,
		stream:    QueryStream,
		root:      path.Join(testBackupID, namespacesDir, testNS, string(QueryStream)),
		loose:     []file{{Path: "s.seg", Size: 64}},
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// Nothing reads from the channel, so a run that did not give up would
	// block here forever.
	out := make(chan Segment)

	if err := s.streamLoose(ctx, &u, out); !errors.Is(err, context.Canceled) {
		t.Errorf("streamLoose() error = %v, want context.Canceled", err)
	}

	data := u.data()
	known := []file{
		// Neither a file of another directory nor one that is not a segment
		// is a segment of this one that no manifest recorded, so neither is
		// sent and neither is stopped by the canceled run.
		{Path: path.Join(data, "p0", "elsewhere.seg"), Size: 64},
		{Path: path.Join(data, "state.bin"), Size: 8},
		{Path: path.Join(data, "unrecorded.seg"), Size: 64},
	}

	err := s.sendUnrecorded(ctx, &u, data, newRecorded(), known, out)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("sendUnrecorded() error = %v, want context.Canceled", err)
	}
}

// A manifest that recorded a segment and then something unusable is reported,
// and reading it again to reconcile the directory it recorded runs into the
// same thing. Nothing can then be said about that directory, so nothing is.
func TestStreamAll_ManifestThatBreaksHalfway(t *testing.T) {
	t.Parallel()

	root := path.Join(testBackupID, namespacesDir, testNS, string(QueryStream))
	good := path.Join(root, dataDir, "p0", "7-0000000000-000000.seg")

	b := newTestBackup(testBackupID)
	b.put(good, segmentBody(64))
	b.put(path.Join(root, dataDir, "p0", "extra.seg"), segmentBody(32))
	b.put(path.Join(root, manifestDir, "0-7-0000181197010.json"), testManifest(testNS, 0, []manifestSegment{
		{SegmentName: good, Size: 64},
		{SegmentName: "../../elsewhere/evil.seg", Size: 64},
	}))

	rootDir := t.TempDir()
	writeBackup(t, rootDir, b)
	s := newTestStreamer(t, &localStore{root: rootDir})

	segments := collect(t, func(out chan<- Segment) error { return s.StreamAll(t.Context(), out) })

	for _, seg := range segments {
		if seg.Unrecorded {
			t.Errorf("segment %q was called unrecorded, want a directory whose manifest "+
				"cannot be read whole to be left alone", seg.Path)
		}
	}

	if s.Stats().ManifestsFailed != 1 {
		t.Errorf("stats = %+v, want the manifest reported as unusable", s.Stats())
	}
}
