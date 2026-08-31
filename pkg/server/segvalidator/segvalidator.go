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

// Package segvalidator proves that an Aerospike server side backup can be read.
// Segments come from a streamers.Streamer, which owns the layout of the storage
// they live in and hands them over either all of them or a random sample, and
// each one is parsed record by record, because a segment that parses is a
// segment a restore can consume.
//
// A segment named by a manifest is checked against what the manifest records:
// one that the storage no longer holds, or that is not the size it was written
// with, is a broken backup even though nothing failed to parse.
//
// A backup is terabytes spread over billions of segments, so nothing here
// scales with their number. Segments are checked as the streamer names them and
// nothing but the segment being parsed is held.
package segvalidator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aerospike/backup-go/pkg/server/segvalidator/models"
	"github.com/aerospike/backup-go/pkg/server/segvalidator/segment"
	"github.com/aerospike/backup-go/pkg/server/segvalidator/streamers"
	"golang.org/x/sync/errgroup"
)

// CheckAll can be passed as the sampleSize argument of Validate to check every
// segment of a backup instead of a random sample.
const CheckAll = 0

// segmentSizeLimit is the hard upper bound of a single segment payload. It is
// also the largest record a flat header can describe, so anything above it
// cannot be a valid segment.
const segmentSizeLimit = segment.MaxRecordSize

// defaultMaxIssues is the number of failures a report describes before it
// starts counting them only.
const defaultMaxIssues = 1000

var (
	// ErrNoSegments is returned when a run checked no segment at all, which
	// also covers a backup id that does not exist.
	ErrNoSegments = errors.New("no segments found for backup")
	// ErrSegmentTooLarge is returned for a segment bigger than any record a
	// flat header can describe.
	ErrSegmentTooLarge = errors.New("segment is too large")
	// ErrSizeMismatch is returned for a segment whose stored size is not the
	// one its manifest records.
	ErrSizeMismatch = errors.New("segment size does not match the manifest")
	// ErrChecksumMismatch is returned for a segment whose bytes do not
	// checksum to what its manifest records. It is the only check that catches
	// a segment whose contents rotted without its structure breaking.
	ErrChecksumMismatch = errors.New("segment checksum does not match the manifest")
)

// Streamer names the segments of one backup and opens them. It is what a
// streamers.Streamer does, named here because this is where it is used.
type Streamer interface {
	// BackupID is the backup the streamer covers.
	BackupID() string
	// StreamAll sends every segment of the backup to out and closes it.
	StreamAll(ctx context.Context, out chan<- streamers.Segment) error
	// StreamSample sends n segments picked at random to out and closes it.
	StreamSample(ctx context.Context, n int, out chan<- streamers.Segment) error
	// OpenSegment downloads the payload of one segment.
	OpenSegment(ctx context.Context, seg *streamers.Segment) (io.ReadCloser, error)
	// Stats is what the streamer learned about the backup on its way.
	Stats() streamers.Stats
}

// SegValidator validates the contents of a backup.
type SegValidator struct {
	streamer Streamer
	logger   *slog.Logger

	// bufPool recycles the per-segment read buffers. Segments are bounded by
	// segmentSizeLimit, so the pool holds at most parallel buffers of that size.
	bufPool sync.Pool

	// parallel bounds the number of segments checked at once.
	parallel int
	// maxIssues bounds the number of issues a report carries.
	maxIssues int
}

// Option configures a SegValidator.
type Option func(*SegValidator)

// WithLogger sets the logger. A nil logger is ignored.
func WithLogger(logger *slog.Logger) Option {
	return func(v *SegValidator) {
		if logger != nil {
			v.logger = logger
		}
	}
}

// WithParallel sets the number of segments checked at once. Values below one
// are ignored.
func WithParallel(n int) Option {
	return func(v *SegValidator) {
		if n > 0 {
			v.parallel = n
		}
	}
}

// WithMaxIssues sets the number of failures a report describes. The ones beyond
// it are still counted. Values below one are ignored.
func WithMaxIssues(n int) Option {
	return func(v *SegValidator) {
		if n > 0 {
			v.maxIssues = n
		}
	}
}

// NewSegValidator creates a validator reading its segments from streamer, which
// covers the one backup the validator checks. By default it checks as many
// segments at once as the machine has CPUs and logs nothing.
func NewSegValidator(streamer Streamer, opts ...Option) (*SegValidator, error) {
	if streamer == nil {
		return nil, errors.New("streamer must not be nil")
	}

	v := &SegValidator{
		streamer: streamer,
		// nil-safe default, so a missing logger never panics.
		logger:    slog.New(slog.DiscardHandler),
		parallel:  max(runtime.NumCPU(), 1),
		maxIssues: defaultMaxIssues,
		bufPool: sync.Pool{
			New: func() any { return new(bytes.Buffer) },
		},
	}

	for _, opt := range opts {
		opt(v)
	}

	return v, nil
}

// Validate checks the backup the streamer covers.
//
// If sampleSize is CheckAll, every segment of the backup is parsed. Otherwise
// the streamer picks sampleSize segments at random and only those are
// downloaded, which is what makes a spot check of a backup of any size cost the
// same.
//
// Anything wrong with a segment is recorded in the report rather than aborting
// the run; only a canceled context and a storage that stops answering stop it
// early.
func (v *SegValidator) Validate(ctx context.Context, sampleSize int) (*models.ValidationReport, error) {
	c := newCollector(v.maxIssues)

	// The streamer fills a channel a pool of workers drains, so a slow check
	// throttles the streaming instead of letting segments pile up in memory.
	segments := make(chan streamers.Segment, v.parallel)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if sampleSize <= CheckAll {
			return v.streamer.StreamAll(gctx, segments)
		}

		return v.streamer.StreamSample(gctx, sampleSize, segments)
	})

	for range v.parallel {
		g.Go(func() error {
			for seg := range segments {
				v.checkSegment(gctx, &seg, c)

				if err := gctx.Err(); err != nil {
					return err
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	if c.checkedSegments.Load() == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNoSegments, v.streamer.BackupID())
	}

	return c.report(v.streamer.BackupID(), v.streamer.Stats()), nil
}

// checkSegment reads a single segment and records what it found. A segment that
// fails becomes an issue rather than an error: one broken segment must not end
// the run.
func (v *SegValidator) checkSegment(ctx context.Context, seg *streamers.Segment, c *collector) {
	// Statistics are collected even when the segment breaks halfway: the
	// records read before the failure are real.
	stats, err := v.parseSegment(ctx, seg)

	c.addSegment(seg, stats, err)

	if err != nil {
		v.logger.DebugContext(ctx, "segment failed validation",
			slog.String("namespace", seg.Namespace),
			slog.String("segment", seg.Path),
			slog.Any("error", err),
		)
	}
}

// parseSegment downloads one segment and walks its records. Partial statistics
// are returned together with the error.
func (v *SegValidator) parseSegment(ctx context.Context, seg *streamers.Segment) (segment.Stats, error) {
	body, err := v.streamer.OpenSegment(ctx, seg)
	if err != nil {
		return segment.Stats{}, fmt.Errorf("failed to open segment: %w", err)
	}
	defer body.Close()

	buf, ok := v.bufPool.Get().(*bytes.Buffer)
	if !ok {
		buf = new(bytes.Buffer)
	}

	defer func() {
		buf.Reset()
		v.bufPool.Put(buf)
	}()

	// Read one byte past the limit, so an oversized segment is detected instead
	// of being silently truncated into a valid looking one.
	if _, err := buf.ReadFrom(io.LimitReader(body, segmentSizeLimit+1)); err != nil {
		return segment.Stats{}, fmt.Errorf("failed to read segment: %w", err)
	}

	if buf.Len() > segmentSizeLimit {
		return segment.Stats{}, fmt.Errorf("%w: over %d bytes", ErrSegmentTooLarge, segmentSizeLimit)
	}

	if err := checkAgainstManifest(seg, buf.Bytes()); err != nil {
		return segment.Stats{}, err
	}

	return segment.Validate(buf.Bytes())
}

// checkAgainstManifest compares a segment with what the manifest that named it
// records. A segment nothing recorded has only itself to be compared against,
// and is left to the parser.
//
// The checksum is what makes this worth doing: a record whose contents rotted
// still parses, because a parser walks sizes and markers rather than data, and
// only the checksum the backup wrote says the bytes came back as they went in.
func checkAgainstManifest(seg *streamers.Segment, payload []byte) error {
	if seg.Manifest == "" {
		return nil
	}

	if seg.Size > 0 && int64(len(payload)) != seg.Size {
		return fmt.Errorf("%w: recorded %d bytes, stored %d", ErrSizeMismatch, seg.Size, len(payload))
	}

	if seg.Checksum == "" {
		return nil
	}

	if sum := fmt.Sprintf("%08x", crc32.ChecksumIEEE(payload)); !strings.EqualFold(sum, seg.Checksum) {
		return fmt.Errorf("%w: recorded %s, stored %s", ErrChecksumMismatch, seg.Checksum, sum)
	}

	return nil
}

// collector gathers the outcome of every check. It is safe for concurrent use,
// and it keeps at most maxIssues issues of each kind, so a backup that is broken
// from end to end cannot exhaust memory through its own report.
type collector struct {
	mu             sync.Mutex
	issues         []models.ValidationIssue
	manifestIssues []models.ManifestIssue
	maxIssues      int

	unrecorded        []string
	checkedSegments   atomic.Int64
	invalidSegments   atomic.Int64
	recordedSegments  atomic.Int64
	unrecordedCount   atomic.Int64
	missingSegments   atomic.Int64
	manifestProblems  atomic.Int64
	records           atomic.Int64
	parsedBytes       atomic.Int64
	skippedCompressed atomic.Int64
}

// newCollector creates a collector describing at most maxIssues failures of
// each kind.
func newCollector(maxIssues int) *collector {
	return &collector{maxIssues: maxIssues}
}

// addSegment records the outcome of checking one segment.
func (c *collector) addSegment(seg *streamers.Segment, stats segment.Stats, err error) {
	c.checkedSegments.Add(1)
	c.records.Add(int64(stats.RecordCount))
	c.parsedBytes.Add(int64(stats.ByteCount))
	c.skippedCompressed.Add(int64(stats.SkippedCompressed))

	if seg.Manifest != "" {
		c.recordedSegments.Add(1)
	}

	if seg.Unrecorded {
		c.addUnrecorded(seg)
	}

	if err == nil {
		return
	}

	c.invalidSegments.Add(1)

	// A segment a manifest named that the storage does not hold, or does not
	// hold whole, says something about the manifest rather than about the
	// segment, and is reported next to the manifest that named it.
	if seg.Manifest != "" && isManifestFailure(err) {
		if errors.Is(err, streamers.ErrSegmentMissing) {
			c.missingSegments.Add(1)
		}

		c.addManifestIssue(seg.Namespace, seg.Manifest, seg.Path, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.issues) < c.maxIssues {
		c.issues = append(c.issues, newIssue(seg, err))
	}
}

// isManifestFailure reports whether a failure is one that says something about
// the manifest that named a segment rather than about the segment itself.
func isManifestFailure(err error) bool {
	return errors.Is(err, streamers.ErrSegmentMissing) ||
		errors.Is(err, ErrSizeMismatch) ||
		errors.Is(err, ErrChecksumMismatch)
}

// addUnrecorded records one segment the storage holds that no manifest names,
// describing at most maxIssues of them and counting the rest.
func (c *collector) addUnrecorded(seg *streamers.Segment) {
	c.unrecordedCount.Add(1)

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.unrecorded) < c.maxIssues {
		c.unrecorded = append(c.unrecorded, seg.Path)
	}
}

// addManifestIssue records one mismatch between a manifest and the storage.
// segmentPath is empty when the manifest itself could not be read.
func (c *collector) addManifestIssue(namespace, manifestPath, segmentPath string, err error) {
	c.manifestProblems.Add(1)

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.manifestIssues) < c.maxIssues {
		c.manifestIssues = append(c.manifestIssues, models.ManifestIssue{
			Err:          err,
			Namespace:    namespace,
			ManifestPath: manifestPath,
			SegmentPath:  segmentPath,
		})
	}
}

// report turns what was collected, together with what the streamer saw on its
// way, into a report.
func (c *collector) report(backupID string, streamed streamers.Stats) *models.ValidationReport {
	c.mu.Lock()
	defer c.mu.Unlock()

	checked := c.checkedSegments.Load()
	invalid := c.invalidSegments.Load()

	// A manifest the streamer could not read is a problem of the backup like
	// any other, and it is the streamer that met it.
	manifestIssues := c.manifestIssues

	for _, issue := range streamed.ManifestIssues {
		if len(manifestIssues) >= c.maxIssues {
			break
		}

		manifestIssues = append(manifestIssues, models.ManifestIssue{
			Err:          issue.Err,
			Namespace:    issue.Namespace,
			ManifestPath: issue.Path,
		})
	}

	return &models.ValidationReport{
		BackupID:          backupID,
		Issues:            c.issues,
		TotalSegments:     streamed.Segments,
		CheckedSegments:   checked,
		ValidSegments:     checked - invalid,
		InvalidSegments:   invalid,
		TotalRecords:      c.records.Load(),
		TotalBytes:        c.parsedBytes.Load(),
		SkippedCompressed: c.skippedCompressed.Load(),
		Manifests: models.ManifestReport{
			Issues:          manifestIssues,
			Total:           streamed.ManifestsFound,
			Checked:         streamed.ManifestsRead,
			CheckedSegments: c.recordedSegments.Load(),
			MissingSegments: c.missingSegments.Load(),
			Unrecorded:      c.unrecordedCount.Load(),

			UnrecordedExamples: c.unrecorded,

			Problems: c.manifestProblems.Load() + streamed.ManifestsFailed,
		},
	}
}

// newIssue describes a failed segment, pointing at the offending record when
// the failure came from parsing one.
func newIssue(seg *streamers.Segment, err error) models.ValidationIssue {
	issue := models.ValidationIssue{
		Err:         err,
		Namespace:   seg.Namespace,
		SegmentPath: seg.Path,
		RecordIndex: models.UnknownRecordIndex,
	}

	var recErr *segment.RecordError
	if errors.As(err, &recErr) {
		issue.RecordIndex = recErr.Index
		issue.Offset = recErr.Offset
	}

	return issue
}
