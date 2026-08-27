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
	"math/rand/v2"
)

// errStopListing ends a listing early without failing it. A sampler asks for a
// page or two of a directory that may hold millions of files, and this is how
// it says it has seen enough.
var errStopListing = errors.New("listing stopped")

// ErrSegmentMissing is returned by OpenSegment for a segment the storage does
// not hold. A manifest naming a segment that is gone is the one failure a
// validator cannot find by reading segments alone.
var ErrSegmentMissing = errors.New("segment does not exist")

// file is one file of a backup, as a listing describes it: where it is and how
// big the storage says it is. Nothing is downloaded to produce one.
type file struct {
	// Path locates the file from the root of the storage, slash separated and
	// starting with the backup id.
	Path string
	// Size is the size the storage reports, in bytes.
	Size int64
}

// levelEntry is one entry of a directory: a directory below it, or a file in
// it. What is below a directory of a level is not part of the level.
type levelEntry struct {
	file
	// Name is the entry within its directory, with no path in front of it.
	Name string
	// IsDir tells a directory from a file.
	IsDir bool
}

// store is the storage a backup lives in, reduced to the three operations this
// package needs. A local directory and an object storage differ in how they
// answer them and in nothing else, so the layout of a backup, the walking of it
// and the sampling of it are written once, above this interface.
//
// Implementations are safe for concurrent use, and every path is slash
// separated and relative to the root of the storage.
type store interface {
	// listLevel calls fn for every entry directly below dir, the directories
	// of that level included and whatever is below them excluded, and stops
	// without failing when fn returns errStopListing. A directory that does
	// not exist yields nothing.
	//
	// This is how the shape of a backup is discovered: naming the partitions
	// of a stream costs one request per thousand of them, whatever the number
	// of segments below them.
	listLevel(ctx context.Context, dir string, fn func(levelEntry) error) error

	// listFiles calls fn for every file below dir, recursively, and stops
	// without failing when fn returns errStopListing. A directory that does
	// not exist yields nothing.
	listFiles(ctx context.Context, dir string, fn func(file) error) error

	// open reads one file, returning ErrSegmentMissing when there is no such
	// file. It is the only operation of this interface that transfers the
	// contents of a backup.
	open(ctx context.Context, path string) (io.ReadCloser, error)
}

// stopped turns the way a caller says it has seen enough into the end of a
// listing, and leaves every other error alone.
func stopped(err error) error {
	if errors.Is(err, errStopListing) {
		return nil
	}

	return err
}

// listDirs collects the names of the directories directly below dir.
func listDirs(ctx context.Context, st store, dir string) ([]string, error) {
	var dirs []string

	err := st.listLevel(ctx, dir, func(entry levelEntry) error {
		if entry.IsDir {
			dirs = append(dirs, entry.Name)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", dir, err)
	}

	return dirs, nil
}

// send hands v to ch, giving up when ctx is done, so a canceled run never
// blocks on a consumer that stopped reading.
func send[T any](ctx context.Context, ch chan<- T, v T) error {
	select {
	case ch <- v:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// maxReservoirPrealloc bounds what a reservoir reserves before it has been
// offered anything.
const maxReservoirPrealloc = 1024

// reservoir keeps a uniform random sample of a stream whose length is unknown
// until it ends. Every value offered has the same chance of being in the
// sample, and nothing but the sample is kept, so a directory of any size costs
// n values of memory.
type reservoir[T any] struct {
	rnd    *rand.Rand
	picked []T
	n      int
	seen   int64
}

// newReservoir creates a reservoir of n values drawing its randomness from rnd.
// The sample grows into whatever it is offered rather than being reserved
// upfront, so asking for a sample far larger than the directory it is drawn
// from costs what the directory holds and not what was asked for.
func newReservoir[T any](n int, rnd *rand.Rand) *reservoir[T] {
	return &reservoir[T]{rnd: rnd, picked: make([]T, 0, min(max(n, 0), maxReservoirPrealloc)), n: n}
}

// offer submits one value of the stream to the sample.
func (r *reservoir[T]) offer(v T) {
	if len(r.picked) < r.n {
		r.picked = append(r.picked, v)
	} else if i := r.rnd.Int64N(r.seen + 1); i < int64(r.n) {
		// The value takes the place of one already picked, with the
		// probability that keeps the sample uniform.
		r.picked[i] = v
	}

	r.seen++
}

// result returns the sample.
func (r *reservoir[T]) result() []T {
	return r.picked
}

// share is the part of what is left of a quota that the next of several sources
// is asked for. A source that has less to give than its share leaves the rest to
// the sources after it.
func share(remaining, sources int) int {
	if sources <= 1 {
		return remaining
	}

	return (remaining + sources - 1) / sources
}

// substream returns the random source the nth piece of work of a run draws
// from. Every piece of work owns one, so a sample depends on the seed alone and
// not on the order in which concurrent listings happen to finish.
func substream(seed uint64, n int) *rand.Rand {
	//nolint:gosec // Sampling does not need a cryptographic source.
	return rand.New(rand.NewPCG(seed, uint64(n)))
}

// shuffle reorders a slice in place.
func shuffle[T any](s []T, rnd *rand.Rand) {
	rnd.Shuffle(len(s), func(i, j int) {
		s[i], s[j] = s[j], s[i]
	})
}
