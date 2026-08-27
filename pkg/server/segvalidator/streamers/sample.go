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
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"

	"golang.org/x/sync/errgroup"
)

// goldenRatio spreads the seed of a run over the pieces of work it is made of.
// It is the odd constant a Fibonacci hash multiplies by.
const goldenRatio = 0x9E37_79B9_7F4A_7C15

// StreamSample sends n segments of the backup, picked at random, to out and
// closes it before returning.
//
// Nothing is enumerated to pick them, because a backup holds far too many
// segments to be enumerated for a spot check. The sample is drawn from the
// manifests instead: the manifests of a stream sit together in one directory,
// so a handful of them is picked out of a listing of names, and each one
// downloaded names a whole partition worth of segments to pick from. A stream
// whose manifests are missing or unreadable is sampled from the segments its
// data directories list, so that a backup missing its metadata is still
// checked rather than silently skipped.
//
// The sample is spread over the namespaces and streams of the backup: every one
// of them is asked for its share of n, and what one of them cannot give is
// asked of the next.
func (s *Streamer) StreamSample(ctx context.Context, n int, out chan<- Segment) error {
	defer close(out)

	if n <= 0 {
		return nil
	}

	units, err := s.units(ctx)
	if err != nil {
		return err
	}

	// The units are visited in a random order, so that a sample smaller than
	// the backup is not always drawn from the same corner of it, and the
	// widest of them is visited last, because it is the one able to make up
	// for what the narrower ones could not give.
	shuffle(units, substream(s.options.seed, 0))
	slices.SortStableFunc(units, func(a, b unit) int {
		return cmp.Compare(len(a.partitions), len(b.partitions))
	})

	planned := quotas(units, n)

	// A unit with less to give than it was asked for leaves the rest to the
	// units after it, so a small stream does not shrink the sample.
	var credit int

	for i := range units {
		quota := planned[i] + credit
		if quota == 0 {
			continue
		}

		picked, err := s.sampleUnit(ctx, &units[i], quota, seedFor(s.options.seed, i))
		if err != nil {
			return err
		}

		for _, seg := range picked {
			s.stats.segments.Add(1)

			if err := send(ctx, out, seg); err != nil {
				return err
			}
		}

		credit = quota - len(picked)
	}

	return nil
}

// quotas splits a sample of n segments over the units of a backup.
//
// Most of it goes where most of the data is, which is what a sample meant to
// stand for the backup should do: the units are weighted by the number of
// partitions they hold, so a namespace scanned into four thousand of them
// carries the sample.
//
// A unit is never left with a token segment though. Every one of them is
// guaranteed a quarter of an equal share, because the change stream of a node
// is a single directory however much it holds, and weighing it by its one
// partition would leave a stream of any size checked by one segment. Asking a
// small unit for more than it holds costs nothing: what it cannot give is
// handed to the units after it.
func quotas(units []unit, n int) []int {
	planned := make([]int, len(units))

	// A sample smaller than the number of units cannot cover them all, so it
	// covers the widest of them, where most of the data is.
	if n <= len(units) {
		for _, i := range byWidth(units)[:n] {
			planned[i] = 1
		}

		return planned
	}

	guaranteed := max(n/(4*len(units)), 1)
	partitions := 0

	for i, u := range units {
		planned[i] = guaranteed
		partitions += len(u.partitions)
	}

	rest := n - guaranteed*len(units)
	handed := 0

	for i, u := range units {
		part := rest * len(u.partitions) / partitions
		planned[i] += part
		handed += part
	}

	// What the division dropped goes to the unit holding the most partitions,
	// which is the one the sample is mostly drawn from anyway.
	if handed < rest {
		planned[widest(units)] += rest - handed
	}

	return planned
}

// widest is the unit holding the most partitions.
func widest(units []unit) int {
	return byWidth(units)[0]
}

// byWidth are the units, as indexes into them, from the one holding the most
// partitions to the one holding the fewest.
func byWidth(units []unit) []int {
	order := make([]int, len(units))
	for i := range order {
		order[i] = i
	}

	slices.SortStableFunc(order, func(a, b int) int {
		return cmp.Compare(len(units[b].partitions), len(units[a].partitions))
	})

	return order
}

// sampleUnit picks up to quota segments of one stream of one namespace.
func (s *Streamer) sampleUnit(ctx context.Context, u *unit, quota int, seed uint64) ([]Segment, error) {
	manifests, err := s.sampleManifests(ctx, u, quota, substream(seed, 0))
	if err != nil {
		return nil, err
	}

	if len(manifests) > 0 {
		picked, err := s.sampleFromManifests(ctx, u, manifests, quota, seed)
		if err != nil {
			return nil, err
		}

		if len(picked) > 0 {
			return picked, nil
		}

		s.logger.WarnContext(ctx, "no manifest of the stream could be sampled from, falling back to its data",
			slog.String("namespace", u.namespace),
			slog.String("stream", string(u.stream)),
			slog.String("manifests", u.manifests()),
		)
	}

	return s.sampleFromData(ctx, u, quota, seed)
}

// sampleManifests picks the manifests of one stream the sample is drawn from.
//
// It reads names, not manifests: one listing of a directory that holds one
// manifest per partition is enough to pick from, and the ones picked are the
// only ones downloaded.
func (s *Streamer) sampleManifests(ctx context.Context, u *unit, quota int, rnd *rand.Rand) ([]file, error) {
	// One manifest per segment of the quota covers as many partitions as the
	// quota can, which is what keeps a sample from coming out of a handful of
	// them. A manifest is a few hundred bytes against the segments it leads to,
	// so reading one per segment checked costs next to nothing.
	picked := newReservoir[file](max(quota, 1), rnd)
	scanned := 0

	err := s.store.listFiles(ctx, u.manifests(), func(f file) error {
		if !isManifest(f.Path) {
			return nil
		}

		picked.offer(f)
		s.stats.manifestsFound.Add(1)

		scanned++
		if scanned >= s.options.scanLimit {
			return errStopListing
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list manifests of %s: %w", u.manifests(), err)
	}

	return picked.result(), nil
}

// sampleFromManifests reads the picked manifests and takes the segments of the
// sample out of what they record. The manifests are read in parallel, and each
// one is walked as it arrives, so what is held is the sample and not the
// manifests.
func (s *Streamer) sampleFromManifests(ctx context.Context, u *unit, manifests []file, quota int, seed uint64,
) ([]Segment, error) {
	picked := make([][]Segment, len(manifests))

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.options.concurrency)

	remaining := quota

	for i, m := range manifests {
		part := share(remaining, len(manifests)-i)
		remaining -= part

		if part == 0 {
			continue
		}

		g.Go(func() error {
			segments, err := s.sampleManifest(gctx, u, m, part, substream(seedFor(seed, i), 1))

			switch {
			case errors.Is(err, ErrManifestUnusable), errors.Is(err, ErrSegmentMissing):
				// A manifest that cannot be read is something to report, not a
				// reason to stop: the rest of the backup is still worth
				// checking, and the other manifests still hold a sample.
				s.stats.addManifestIssue(u.namespace, m.Path, err)

				s.logger.DebugContext(gctx, "manifest could not be sampled from",
					slog.String("namespace", u.namespace),
					slog.String("manifest", m.Path),
					slog.Any("error", err),
				)

				return nil
			case err != nil:
				return err
			}

			s.stats.manifestsRead.Add(1)
			picked[i] = segments

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return slices.Concat(picked...), nil
}

// sampleManifest downloads one manifest and picks quota of the segments it
// records.
func (s *Streamer) sampleManifest(ctx context.Context, u *unit, m file, quota int, rnd *rand.Rand,
) ([]Segment, error) {
	body, err := s.store.open(ctx, m.Path)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	recorded := newReservoir[manifestSegment](quota, rnd)
	header := manifestHeader{Namespace: u.namespace}

	err = decodeManifest(body, &header, func(seg manifestSegment) error {
		recorded.offer(seg)

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Unlike a run sending segments as it reads them, a sample is picked once
	// the manifest has been walked, so it is described by all of what the
	// manifest says about itself.
	segments := make([]Segment, 0, len(recorded.result()))

	for _, seg := range recorded.result() {
		picked, err := u.segmentOfRecord(m, header, seg)
		if err != nil {
			return nil, err
		}

		segments = append(segments, picked)
	}

	return segments, nil
}

// sampleFromData picks segments straight out of the data directories of a
// stream, which is what is left when its manifests cannot be used.
//
// The partitions are picked first, out of a listing of their names, and only
// the ones picked are listed. A partition holding more segments than a listing
// is allowed to scan is sampled from the ones it scanned.
func (s *Streamer) sampleFromData(ctx context.Context, u *unit, quota int, seed uint64) ([]Segment, error) {
	partitions := slices.Clone(u.partitions)
	shuffle(partitions, substream(seed, 2))

	// Spreading the quota over as many partitions as it has segments to pick
	// keeps a sample from coming out of a single partition.
	partitions = partitions[:min(len(partitions), max(quota, 1))]
	picked := make([][]Segment, len(partitions))

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.options.concurrency)

	remaining := quota

	for i, partition := range partitions {
		part := share(remaining, len(partitions)-i)
		remaining -= part

		if part == 0 {
			continue
		}

		g.Go(func() error {
			segments, err := s.samplePartition(gctx, u, partition, part, substream(seedFor(seed, i), 3))
			if err != nil {
				return err
			}

			picked[i] = segments

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return slices.Concat(picked...), nil
}

// samplePartition picks quota segments out of the listing of one partition.
func (s *Streamer) samplePartition(ctx context.Context, u *unit, partition string, quota int, rnd *rand.Rand,
) ([]Segment, error) {
	dir := u.partition(partition)
	picked := newReservoir[file](quota, rnd)
	scanned := 0

	err := s.store.listFiles(ctx, dir, func(f file) error {
		if !isSegment(f.Path) {
			return nil
		}

		picked.offer(f)

		scanned++
		if scanned >= s.options.scanLimit {
			return errStopListing
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list segments of %s: %w", dir, err)
	}

	segments := make([]Segment, 0, len(picked.result()))

	for _, f := range picked.result() {
		segments = append(segments, Segment{
			Namespace: u.namespace,
			Stream:    u.stream,
			Path:      f.Path,
			Size:      f.Size,
		})
	}

	return segments, nil
}

// seedFor is the seed the nth piece of work of a run draws from. Deriving one
// per piece of work is what makes a seeded run repeatable: a sample then
// depends on the seed alone, and not on the order in which concurrent listings
// happen to finish.
func seedFor(seed uint64, n int) uint64 {
	return seed ^ (uint64(n)+1)*goldenRatio
}
