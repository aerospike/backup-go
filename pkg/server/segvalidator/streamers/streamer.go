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

// Package streamers names the segments of one Aerospike server side backup and
// opens them, so that a validator can prove the backup reads back.
//
// A Streamer covers a single backup, the one it was created for, and offers the
// two ways of going through it a dry run needs: every segment it holds, or a
// number of segments picked at random. Both are built on the fixed layout of a
// backup, described in layout.go.
//
// A backup is terabytes spread over billions of segments, and nothing here
// scales with that number. Directories are listed, never downloaded, a listing
// is consumed page by page instead of being collected, and a sample is drawn
// from the manifests of a stream, which name a partition worth of segments per
// download, rather than from the segments themselves. The only thing this
// package ever transfers is the manifests it samples from and the segments a
// caller asks it to open.
package streamers

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"path"
	"slices"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

const (
	// defaultConcurrency is tuned for listings and downloads waiting on a
	// network rather than on a CPU.
	defaultConcurrency = 16
	// defaultScanLimit bounds the files one listing looks at while sampling. A
	// sample is drawn from the files scanned, so this is what keeps sampling a
	// bounded amount of work on a directory of any size.
	defaultScanLimit = 100_000
	// maxManifestIssues is the number of unusable manifests a run describes
	// before it starts counting them only.
	maxManifestIssues = 100
	// maxLooseSegments is the number of segments a data directory may hold
	// outside of its partitions before it is walked as one directory instead.
	maxLooseSegments = 4096
)

// Segment locates one segment of a backup and says how big it is meant to be.
type Segment struct {
	// Namespace is the Aerospike namespace the segment belongs to.
	Namespace string
	// Stream is the stream the segment was written by.
	Stream Stream
	// Path locates the segment from the root of the storage.
	Path string
	// Manifest locates the manifest that named this segment, and is empty for a
	// segment found by listing a data directory. A segment that has one has a
	// recorded Size and Checksum worth comparing against what the storage
	// returns.
	Manifest string
	// Checksum is the CRC-32 a manifest records for the segment, as the eight
	// hexadecimal digits it is written in. It is empty for a segment no
	// manifest named, and for one whose manifest checksummed it some other way.
	Checksum string
	// Unrecorded marks a segment the storage holds that no manifest names. It
	// is read like any other, but nothing recorded what it should be, and it is
	// either the leftover of an interrupted flush or the sign of a manifest
	// that never made it.
	Unrecorded bool
	// Size is the size a manifest records for the segment, or the size the
	// storage reported when listing it.
	Size int64
}

// ManifestIssue describes a manifest a run could not sample from.
type ManifestIssue struct {
	// Err is the reason the manifest was not usable.
	Err error
	// Namespace is the namespace the manifest belongs to.
	Namespace string
	// Path locates the manifest.
	Path string
}

// Stats is what a run learned about the backup on its way. A sampled run does
// not enumerate the backup, so these numbers describe what it looked at, never
// what the backup holds.
type Stats struct {
	// ManifestIssues describes the manifests that could not be used, capped at
	// maxManifestIssues. ManifestsFailed counts them all.
	ManifestIssues []ManifestIssue
	// Namespaces is the number of namespaces the backup holds.
	Namespaces int64
	// ManifestsFound is the number of manifests the run listed.
	ManifestsFound int64
	// ManifestsRead is the number of manifests the run downloaded and sampled
	// segments from.
	ManifestsRead int64
	// ManifestsFailed is the number of manifests that could not be used.
	ManifestsFailed int64
	// Segments is the number of segments the run named to its caller.
	Segments int64
	// Unrecorded is the number of them that no manifest names.
	Unrecorded int64
}

// Streamer streams the segments of one backup out of the storage holding it.
// One backup is validated at a time, so the backup is fixed when the streamer
// is created and every path it produces belongs to it.
type Streamer struct {
	store    store
	logger   *slog.Logger
	backupID string
	stats    stats
	options  Options
}

// Options are the settings of a Streamer.
type Options struct {
	// concurrency bounds the listings and manifest downloads running at once.
	concurrency int
	// scanLimit bounds the files one listing looks at while sampling.
	scanLimit int
	// seed is what the sampling draws its randomness from.
	seed uint64
}

// Option configures a Streamer.
type Option func(*Streamer)

// WithLogger sets the logger. A nil logger is ignored.
func WithLogger(logger *slog.Logger) Option {
	return func(s *Streamer) {
		if logger != nil {
			s.logger = logger
		}
	}
}

// WithConcurrency sets the number of listings and manifest downloads running at
// once. Values below one are ignored.
func WithConcurrency(n int) Option {
	return func(s *Streamer) {
		if n > 0 {
			s.options.concurrency = n
		}
	}
}

// WithScanLimit sets how many files a single listing looks at while sampling. A
// sample is drawn from the files scanned, so a higher limit spreads it wider
// and costs more listing requests. Values below one are ignored.
func WithScanLimit(n int) Option {
	return func(s *Streamer) {
		if n > 0 {
			s.options.scanLimit = n
		}
	}
}

// WithSeed fixes what the sampling draws its randomness from, which makes a
// sampled run repeatable. By default every run picks its own seed.
func WithSeed(seed uint64) Option {
	return func(s *Streamer) {
		s.options.seed = seed
	}
}

// newStreamer creates a streamer over one backup of a storage.
func newStreamer(st store, backupID string, opts ...Option) (*Streamer, error) {
	if backupID == "" {
		return nil, errors.New("backup id must not be empty")
	}

	s := &Streamer{
		store:    st,
		backupID: backupID,
		// nil-safe default, so a missing logger never panics.
		logger: slog.New(slog.DiscardHandler),
		options: Options{
			concurrency: defaultConcurrency,
			scanLimit:   defaultScanLimit,
			//nolint:gosec // Sampling does not need a cryptographic source.
			seed: rand.Uint64(),
		},
	}

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

// BackupID is the backup this streamer covers.
func (s *Streamer) BackupID() string {
	return s.backupID
}

// Stats reports what the run learned about the backup. It is meant to be read
// once streaming is over.
func (s *Streamer) Stats() Stats {
	return s.stats.snapshot()
}

// OpenSegment downloads the payload of one segment, and returns
// ErrSegmentMissing when the storage does not hold it, which is what a manifest
// naming a segment that is gone looks like. The caller closes the reader.
func (s *Streamer) OpenSegment(ctx context.Context, seg *Segment) (io.ReadCloser, error) {
	return s.store.open(ctx, seg.Path)
}

// StreamAll sends every segment of the backup to out and closes it before
// returning. Segments arrive in an unspecified order.
//
// They come from the manifests, which is what makes a full run worth more than
// reading whatever the storage happens to hold: a manifest says how big a
// segment was written and what it checksummed to, and it names the segments a
// restore will look for, so a segment that went missing is missing from the
// listing but not from the manifest that promised it.
//
// A stream whose manifests are missing or unreadable falls back to the segments
// its data directories list, so that a backup missing its metadata is still
// read rather than silently skipped.
//
// Nothing but the manifests is downloaded here, and a manifest is walked as it
// arrives. The work of one stream runs in parallel and a slow consumer
// throttles it, so a backup of any size is walked at the speed it is checked
// and never piles up in memory.
func (s *Streamer) StreamAll(ctx context.Context, out chan<- Segment) error {
	defer close(out)

	units, err := s.units(ctx)
	if err != nil {
		return err
	}

	// The units are walked one after the other, each of them using the whole
	// concurrency, because whether a unit falls back to its data directories
	// is only known once its manifests have been read.
	for i := range units {
		if err := s.streamUnit(ctx, &units[i], out); err != nil {
			return err
		}
	}

	return nil
}

// streamUnit sends every segment of one stream of one namespace: the ones its
// manifests record, and then the ones the storage holds that they do not.
func (s *Streamer) streamUnit(ctx context.Context, u *unit, out chan<- Segment) error {
	rec := newRecorded()

	recorded, err := s.streamManifests(ctx, u, rec, out)
	if err != nil {
		return err
	}

	if recorded > 0 {
		return s.streamUnrecorded(ctx, u, rec, out)
	}

	s.logger.WarnContext(ctx, "no manifest of the stream could be read, falling back to its data",
		slog.String("namespace", u.namespace),
		slog.String("stream", string(u.stream)),
		slog.String("manifests", u.manifests()),
	)

	return s.streamData(ctx, u, out)
}

// streamManifests reads every manifest of a unit and sends the segments they
// record, returning how many were sent.
//
// The manifests are read in parallel as they are listed, and the listing is
// paced by the reading, so a stream with a manifest per partition is walked
// without its names being collected first.
func (s *Streamer) streamManifests(ctx context.Context, u *unit, rec *recorded, out chan<- Segment,
) (int64, error) {
	var recorded atomic.Int64

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.options.concurrency)

	listErr := s.store.listFiles(ctx, u.manifests(), func(f file) error {
		if !isManifest(f.Path) {
			return nil
		}

		s.stats.manifestsFound.Add(1)

		g.Go(func() error {
			sent, err := s.streamManifest(gctx, u, f, rec, out)
			recorded.Add(sent)

			if err != nil {
				return s.manifestFailed(gctx, u, f, err)
			}

			s.stats.manifestsRead.Add(1)

			return nil
		})

		// A manifest that failed to be read ends the listing, so the failure
		// is not chased with more work. The failure itself comes out of the
		// group, which is why the listing merely stops here.
		if gctx.Err() != nil {
			return errStopListing
		}

		return nil
	})

	if err := g.Wait(); err != nil {
		return recorded.Load(), err
	}

	// A run canceled before anything failed has nothing to report but that.
	if err := ctx.Err(); err != nil {
		return recorded.Load(), err
	}

	if listErr != nil {
		return recorded.Load(), fmt.Errorf("list manifests of %s: %w", u.manifests(), listErr)
	}

	return recorded.Load(), nil
}

// streamManifest reads one manifest and sends the segments it records.
//
// The segments are sent as the manifest is walked, so a manifest recording a
// million of them costs one of them in memory. That is also why what a manifest
// says about itself is taken as it is met: a manifest records its namespace and
// how it checksummed its segments before recording the segments themselves,
// and a segment met before them is described by the little that is known.
func (s *Streamer) streamManifest(ctx context.Context, u *unit, m file, rec *recorded, out chan<- Segment,
) (int64, error) {
	body, err := s.store.open(ctx, m.Path)
	if err != nil {
		return 0, err
	}
	defer body.Close()

	var sent int64

	header := manifestHeader{Namespace: u.namespace}

	err = decodeManifest(body, &header, func(recorded manifestSegment) error {
		seg, err := u.segmentOfRecord(m, header, recorded)
		if err != nil {
			return err
		}

		s.stats.segments.Add(1)

		sent++

		// What the manifests recorded of a directory is what the storage is
		// held against once they have all been read.
		rec.add(path.Dir(seg.Path), m.Path)

		return send(ctx, out, seg)
	})
	if err != nil {
		return sent, err
	}

	return sent, nil
}

// manifestFailed records a manifest that could not be read. A broken manifest
// is something to report, not a reason to stop: the rest of the backup is still
// worth checking.
func (s *Streamer) manifestFailed(ctx context.Context, u *unit, m file, err error) error {
	if !errors.Is(err, ErrManifestUnusable) && !errors.Is(err, ErrSegmentMissing) {
		return err
	}

	s.stats.addManifestIssue(u.namespace, m.Path, err)

	s.logger.DebugContext(ctx, "manifest could not be read",
		slog.String("namespace", u.namespace),
		slog.String("manifest", m.Path),
		slog.Any("error", err),
	)

	return nil
}

// streamData sends the segments the data directories of a unit list, which is
// what is left when its manifests cannot be used.
func (s *Streamer) streamData(ctx context.Context, u *unit, out chan<- Segment) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.options.concurrency)

	for _, partition := range u.partitions {
		g.Go(func() error {
			return s.streamPartition(gctx, u, partition, out)
		})
	}

	if len(u.loose) > 0 {
		g.Go(func() error {
			return s.streamLoose(gctx, u, out)
		})
	}

	return g.Wait()
}

// streamLoose sends the segments a data directory holds outside of its
// partitions, which were named while the partitions were.
func (s *Streamer) streamLoose(ctx context.Context, u *unit, out chan<- Segment) error {
	for _, f := range u.loose {
		s.stats.segments.Add(1)

		if err := send(ctx, out, u.segmentOf(f)); err != nil {
			return err
		}
	}

	return nil
}

// streamPartition sends every segment of one partition to out.
func (s *Streamer) streamPartition(ctx context.Context, u *unit, partition string, out chan<- Segment) error {
	dir := u.partition(partition)

	err := s.store.listFiles(ctx, dir, func(f file) error {
		if !isSegment(f.Path) {
			return nil
		}

		s.stats.segments.Add(1)

		return send(ctx, out, u.segmentOf(f))
	})
	if err != nil {
		return fmt.Errorf("list segments of %s: %w", dir, err)
	}

	return nil
}

// unit is the smallest self contained part of a backup: a directory holding the
// segments of one stream of one namespace and the manifests describing them.
//
// A query stream is one unit, while a change stream is one unit per node, and
// what a unit is made of is discovered rather than assumed.
type unit struct {
	namespace string
	stream    Stream
	// root holds the data and manifest directories.
	root string
	// partitions are the directories the segments are grouped in, or a single
	// empty name for a stream whose segments are walked as one directory.
	partitions []string
	// loose are the segments of the data directory itself, which a stream
	// grouping its segments in partitions is not expected to have. They were
	// named while the partitions were, so they cost no listing of their own.
	loose []file
}

// data locates the directory holding the segments of the unit.
func (u *unit) data() string {
	return path.Join(u.root, dataDir)
}

// manifests locates the directory holding the manifests of the unit.
func (u *unit) manifests() string {
	return path.Join(u.root, manifestDir)
}

// partition locates one partition directory of the unit.
func (u *unit) partition(name string) string {
	return path.Join(u.data(), name)
}

// segmentOf describes a listed file of the unit as a segment. It has the size
// the listing reported and no manifest, because nothing recorded it: it is
// there.
func (u *unit) segmentOf(f file) Segment {
	return Segment{
		Namespace: u.namespace,
		Stream:    u.stream,
		Path:      f.Path,
		Size:      f.Size,
	}
}

// segmentOfRecord describes a segment a manifest records. It carries what the
// manifest promised of it, which is what a validator compares the storage
// against, and it is a segment whether or not the storage still holds it.
func (u *unit) segmentOfRecord(m file, header manifestHeader, recorded manifestSegment) (Segment, error) {
	segmentPath, err := recorded.resolve(u.data(), header.Partition)
	if err != nil {
		return Segment{}, err
	}

	seg := Segment{
		// The manifest knows which namespace it describes; the directory it
		// was found in is only what that namespace is called in the storage.
		Namespace: cmp.Or(header.Namespace, u.namespace),
		Stream:    u.stream,
		Path:      segmentPath,
		Manifest:  m.Path,
		Size:      recorded.Size,
	}

	// A checksum of an algorithm this package cannot compute is worse than no
	// checksum: it would fail every segment it describes.
	if header.Algorithm == crc32Algorithm {
		seg.Checksum = recorded.Checksum
	}

	return seg, nil
}

// units discovers what the backup is made of: its namespaces, the streams of
// each of them and the partitions of each stream.
//
// This is the whole cost of finding the shape of a backup, and it does not
// depend on the number of segments: one listing names the namespaces, one names
// the streams, and one names the partitions of a stream.
func (s *Streamer) units(ctx context.Context) ([]unit, error) {
	namespaces, err := listDirs(ctx, s.store, namespacesRoot(s.backupID))
	if err != nil {
		return nil, fmt.Errorf("backup %s: %w", s.backupID, err)
	}

	s.stats.namespaces.Store(int64(len(namespaces)))

	var units []unit

	for _, namespace := range namespaces {
		for _, stream := range streams {
			found, err := s.streamUnits(ctx, namespace, stream)
			if err != nil {
				return nil, err
			}

			units = append(units, found...)
		}
	}

	return units, nil
}

// streamUnits discovers the units of one stream of one namespace. A stream
// holding its data directory itself is one unit; a stream holding a directory
// per node is one unit per node.
func (s *Streamer) streamUnits(ctx context.Context, namespace string, stream Stream) ([]unit, error) {
	root := streamRoot(s.backupID, namespace, stream)

	children, err := listDirs(ctx, s.store, root)
	if err != nil {
		return nil, err
	}

	if slices.Contains(children, dataDir) {
		u, err := s.newUnit(ctx, namespace, stream, root)
		if err != nil {
			return nil, err
		}

		return []unit{u}, nil
	}

	units := make([]unit, 0, len(children))

	for _, child := range children {
		if child == manifestDir {
			continue
		}

		sub := path.Join(root, child)

		grandChildren, err := listDirs(ctx, s.store, sub)
		if err != nil {
			return nil, err
		}

		// Anything else a stream directory may hold is not a unit and is left
		// alone rather than guessed at.
		if !slices.Contains(grandChildren, dataDir) {
			continue
		}

		u, err := s.newUnit(ctx, namespace, stream, sub)
		if err != nil {
			return nil, err
		}

		units = append(units, u)
	}

	return units, nil
}

// newUnit reads one level of the data directory of a unit, which tells how its
// segments are laid out: grouped in partition directories, as a query stream
// writes them, or sitting in the data directory itself, as a change stream
// does.
func (s *Streamer) newUnit(ctx context.Context, namespace string, stream Stream, root string) (unit, error) {
	u := unit{namespace: namespace, stream: stream, root: root}
	flat := false

	err := s.store.listLevel(ctx, u.data(), func(entry levelEntry) error {
		if entry.IsDir {
			u.partitions = append(u.partitions, entry.Name)

			return nil
		}

		if !isSegment(entry.Path) {
			return nil
		}

		// A data directory holding more segments than a level is worth
		// walking is one whose segments are not grouped at all, and walking
		// the rest of it here would cost what walking the backup costs.
		if len(u.loose) >= maxLooseSegments {
			flat = true

			return errStopListing
		}

		u.loose = append(u.loose, entry.file)

		return nil
	})
	if err != nil {
		return u, fmt.Errorf("list %s: %w", u.data(), err)
	}

	if flat || len(u.partitions) == 0 {
		// The data directory is walked as a whole, which covers the segments
		// sitting in it and anything grouped below it.
		u.partitions = []string{""}
		u.loose = nil
	}

	return u, nil
}

// stats counts what a run went through. It is written by every goroutine of a
// run and read once it is over.
type stats struct {
	mu     sync.Mutex
	issues []ManifestIssue

	namespaces      atomic.Int64
	manifestsFound  atomic.Int64
	manifestsRead   atomic.Int64
	manifestsFailed atomic.Int64
	segments        atomic.Int64
	unrecorded      atomic.Int64
}

// addManifestIssue records a manifest that could not be used, describing the
// first maxManifestIssues of them and counting the rest.
func (s *stats) addManifestIssue(namespace, manifestPath string, err error) {
	s.manifestsFailed.Add(1)

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.issues) < maxManifestIssues {
		s.issues = append(s.issues, ManifestIssue{
			Err:       err,
			Namespace: namespace,
			Path:      manifestPath,
		})
	}
}

// snapshot copies what has been counted so far.
func (s *stats) snapshot() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	return Stats{
		ManifestIssues:  slices.Clone(s.issues),
		Namespaces:      s.namespaces.Load(),
		ManifestsFound:  s.manifestsFound.Load(),
		ManifestsRead:   s.manifestsRead.Load(),
		ManifestsFailed: s.manifestsFailed.Load(),
		Segments:        s.segments.Load(),
		Unrecorded:      s.unrecorded.Load(),
	}
}
