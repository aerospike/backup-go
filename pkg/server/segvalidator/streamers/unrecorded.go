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
	"fmt"
	"hash/maphash"
	"log/slog"
	"path"
	"slices"
	"sync"

	"golang.org/x/sync/errgroup"
)

// A full run reads what the manifests promise, and then makes sure the storage
// holds nothing beyond it. A segment no manifest names is not part of the
// backup a restore would read: it is the leftover of an interrupted flush, or
// the sign of a manifest that never made it.
//
// Finding them cannot cost a set of every segment of the backup, so it costs a
// count instead. The manifests say how many segments each directory should
// hold, a listing says how many it does, and only a directory holding more than
// it should is looked at closely, by reading its manifests again to name the
// segments they record. What is held is therefore one number per directory,
// plus the contents of the directories that turned out to be wrong.

// recorded is what the manifests of a unit said about the directories of its
// data: how many segments each of them holds, and which manifests to ask again
// when one of them holds more. It is written by every manifest being read at
// once.
type recorded struct {
	mu   sync.Mutex
	dirs map[string]*recordedDir
}

// recordedDir is what the manifests recorded about one directory.
type recordedDir struct {
	// manifests are the manifests that recorded a segment of the directory.
	manifests []string
	// segments is the number of segments they recorded in it.
	segments int
}

// newRecorded creates an empty record.
func newRecorded() *recorded {
	return &recorded{dirs: make(map[string]*recordedDir)}
}

// add notes that a manifest recorded one segment in a directory.
func (r *recorded) add(dir, manifest string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	found, ok := r.dirs[dir]
	if !ok {
		found = &recordedDir{}
		r.dirs[dir] = found
	}

	found.segments++

	// A directory is described by a handful of manifests, and the segments of
	// one manifest arrive together, so this stays a comparison or two.
	if !slices.Contains(found.manifests, manifest) {
		found.manifests = append(found.manifests, manifest)
	}
}

// of returns what was recorded about a directory.
func (r *recorded) of(dir string) recordedDir {
	r.mu.Lock()
	defer r.mu.Unlock()

	found, ok := r.dirs[dir]
	if !ok {
		return recordedDir{}
	}

	return recordedDir{manifests: slices.Clone(found.manifests), segments: found.segments}
}

// streamUnrecorded sends the segments of a unit that no manifest named.
//
// The data directories are listed, which downloads nothing, and their segments
// are counted per directory. A directory holding no more than its manifests
// recorded is left alone; that is the answer for a backup that is in order, and
// it costs one listing and no memory.
func (s *Streamer) streamUnrecorded(ctx context.Context, u *unit, rec *recorded, out chan<- Segment) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.options.concurrency)

	for _, partition := range u.partitions {
		g.Go(func() error {
			return s.countPartition(gctx, u, partition, rec, out)
		})
	}

	// The segments of the data directory itself were named while the
	// partitions were, so they are counted without listing anything again.
	if len(u.loose) > 0 && len(u.loose) > rec.of(u.data()).segments {
		g.Go(func() error {
			return s.sendUnrecorded(gctx, u, u.data(), rec, u.loose, out)
		})
	}

	return g.Wait()
}

// countPartition counts what one partition holds and reconciles the
// directories holding more than their manifests recorded.
func (s *Streamer) countPartition(ctx context.Context, u *unit, partition string, rec *recorded,
	out chan<- Segment,
) error {
	dir := u.partition(partition)
	listed := make(map[string]int)

	err := s.store.listFiles(ctx, dir, func(f file) error {
		if isSegment(f.Path) {
			listed[path.Dir(f.Path)]++
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("list segments of %s: %w", dir, err)
	}

	for found, count := range listed {
		if count <= rec.of(found).segments {
			continue
		}

		if err := s.sendUnrecorded(ctx, u, found, rec, nil, out); err != nil {
			return err
		}
	}

	return nil
}

// sendUnrecorded names the segments of one directory that no manifest recorded
// and sends them.
//
// The manifests of the directory are read again to do it, which is the price of
// not having held what they said the first time. Only a directory that turned
// out to hold more than it should is worth that, and it holds what one
// directory holds.
func (s *Streamer) sendUnrecorded(ctx context.Context, u *unit, dir string, rec *recorded, known []file,
	out chan<- Segment,
) error {
	found := rec.of(dir)
	seed := maphash.MakeSeed()

	names := s.recordedNames(ctx, u, dir, found.manifests, seed)

	// Without the names, every segment of the directory would look like one
	// nothing recorded, which is worse than saying nothing about it.
	if names == nil {
		return nil
	}

	emit := func(f file) error {
		if path.Dir(f.Path) != dir || !isSegment(f.Path) {
			return nil
		}

		if _, ok := names[maphash.String(seed, f.Path)]; ok {
			return nil
		}

		s.stats.unrecorded.Add(1)
		s.stats.segments.Add(1)

		seg := u.segmentOf(f)
		seg.Unrecorded = true

		return send(ctx, out, seg)
	}

	if known != nil {
		for _, f := range known {
			if err := emit(f); err != nil {
				return err
			}
		}

		return nil
	}

	if err := s.store.listFiles(ctx, dir, emit); err != nil {
		return fmt.Errorf("list segments of %s: %w", dir, err)
	}

	return nil
}

// recordedNames reads the manifests of one directory again and returns the
// segments they record in it, as hashes of their paths: what is needed of them
// is whether a listed segment is one of them. It returns nothing at all, and no
// error, for a directory whose manifests cannot be read again, because nothing
// can be concluded about what it holds.
func (s *Streamer) recordedNames(ctx context.Context, u *unit, dir string, manifests []string, seed maphash.Seed,
) map[uint64]struct{} {
	names := make(map[uint64]struct{})

	for _, manifestPath := range manifests {
		err := s.readRecordedNames(ctx, u, dir, file{Path: manifestPath}, seed, names)
		if err != nil {
			// A manifest that was read once and cannot be read again says
			// nothing new about the backup; the segments it recorded stay
			// recorded rather than being reported as belonging to nothing.
			s.logger.DebugContext(ctx, "manifest could not be read again",
				slog.String("namespace", u.namespace),
				slog.String("manifest", manifestPath),
				slog.Any("error", err),
			)

			return nil
		}
	}

	return names
}

// readRecordedNames adds the segments one manifest records in a directory to
// names.
func (s *Streamer) readRecordedNames(ctx context.Context, u *unit, dir string, m file, seed maphash.Seed,
	names map[uint64]struct{},
) error {
	body, err := s.store.open(ctx, m.Path)
	if err != nil {
		return err
	}
	defer body.Close()

	header := manifestHeader{Namespace: u.namespace}

	return decodeManifest(body, &header, func(record manifestSegment) error {
		seg, err := u.segmentOfRecord(m, header, record)
		if err != nil {
			return err
		}

		if path.Dir(seg.Path) == dir {
			names[maphash.String(seed, seg.Path)] = struct{}{}
		}

		return nil
	})
}
