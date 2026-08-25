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

package segvalidator

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/aerospike/backup-go/pkg/server/segvalidator/segment"
	"github.com/aerospike/backup-go/pkg/server/segvalidator/streamers"
)

// fixtureSegmentHex is a one record segment: a 64 byte flat record holding set
// "demo" and a single string bin.
const fixtureSegmentHex = "01f27a03030050000102030405060708090a0b0c0d0e0f10111213140000" +
	"00000001000464656d6f010161030500000068656c6c6fe127ea0700000000000000"

// fixtureCompressedHex is the same record with the is_compressed flag set.
const fixtureCompressedHex = "01f27a030300d0000102030405060708090a0b0c0d0e0f10111213140000" +
	"00000001000464656d6f010161030500000068656c6c6fe127ea0700000000000000"

const (
	stubBackupID = "519118324"
	stubNS       = "source-ns1"

	// fixtureSegmentBytes is the size of the segment fixtures.
	fixtureSegmentBytes = 64
)

var errOpenSegment = errors.New("open failed")

// stubSegmentPath names the nth segment a stubStreamer streams.
func stubSegmentPath(n int) string {
	return fmt.Sprintf("%s/ns/%s/query-stream/data/p%d/s0.seg", stubBackupID, stubNS, n)
}

// stubManifestPath names the manifest a stubStreamer says a segment came from.
func stubManifestPath(n int) string {
	return fmt.Sprintf("%s/ns/%s/query-stream/manifest/%d-0.json", stubBackupID, stubNS, n)
}

// stubStreamer streams segments without holding them: they are generated as
// they are sent, so a test can ask for more of them than would fit in memory.
// Every download it serves is counted, which is how a test proves that a
// sampled run downloads the sample and nothing else.
type stubStreamer struct {
	payload   []byte
	streamErr error
	openFunc  func(path string) (io.ReadCloser, error)
	// missing are the paths the storage does not hold.
	missing map[string]bool
	// recordedSize is the size the manifests claim, when it differs from the
	// size of the payload.
	recordedSize int64
	// recordedChecksum is the CRC-32 the manifests claim.
	recordedChecksum string
	// stats is what the streamer reports having seen.
	stats streamers.Stats
	// segments is the number of segments the backup holds.
	segments int
	// fromManifest makes every segment one a manifest named.
	fromManifest bool
	// unrecorded makes every segment one no manifest names.
	unrecorded bool

	opened atomic.Int64
}

func newStubStreamer(payload []byte, segments int) *stubStreamer {
	return &stubStreamer{payload: payload, segments: segments}
}

func (s *stubStreamer) BackupID() string {
	return stubBackupID
}

func (s *stubStreamer) StreamAll(ctx context.Context, out chan<- streamers.Segment) error {
	return s.stream(ctx, s.segments, out)
}

func (s *stubStreamer) StreamSample(ctx context.Context, n int, out chan<- streamers.Segment) error {
	return s.stream(ctx, min(n, s.segments), out)
}

func (s *stubStreamer) stream(ctx context.Context, count int, out chan<- streamers.Segment) error {
	defer close(out)

	if s.streamErr != nil {
		return s.streamErr
	}

	for i := range count {
		seg := streamers.Segment{
			Namespace: stubNS,
			Stream:    streamers.QueryStream,
			Path:      stubSegmentPath(i),
			Size:      int64(len(s.payload)),
		}

		seg.Unrecorded = s.unrecorded

		if s.fromManifest {
			seg.Manifest = stubManifestPath(i)
			seg.Checksum = s.recordedChecksum

			if s.recordedSize != 0 {
				seg.Size = s.recordedSize
			}
		}

		select {
		case out <- seg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (s *stubStreamer) OpenSegment(_ context.Context, seg *streamers.Segment) (io.ReadCloser, error) {
	s.opened.Add(1)

	if s.missing[seg.Path] {
		return nil, fmt.Errorf("%w: %s", streamers.ErrSegmentMissing, seg.Path)
	}

	if s.openFunc != nil {
		return s.openFunc(seg.Path)
	}

	return io.NopCloser(bytes.NewReader(s.payload)), nil
}

func (s *stubStreamer) Stats() streamers.Stats {
	stats := s.stats
	stats.Segments = int64(s.segments)

	return stats
}

// zeroReader is an endless source of zero bytes.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)

	return len(p), nil
}

// decodeSegmentHex turns a fixture into bytes.
func decodeSegmentHex(t *testing.T, s string) []byte {
	t.Helper()

	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("decode fixture: %v", err)
	}

	return b
}

// breakSegment returns a copy of the segment with a digest byte flipped, which
// breaks the end marker of its first record.
func breakSegment(payload []byte) []byte {
	broken := bytes.Clone(payload)
	broken[8] ^= 0xff

	return broken
}

func newTestSegValidator(t *testing.T, streamer Streamer, opts ...Option) *SegValidator {
	t.Helper()

	v, err := NewSegValidator(streamer, opts...)
	if err != nil {
		t.Fatalf("NewSegValidator() error = %v", err)
	}

	return v
}

func TestSegValidator_ValidateReadableBackup(t *testing.T) {
	t.Parallel()

	const segments = 3

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), segments)

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if report.Failed() {
		t.Fatalf("Validate() reported issues: %+v", report.Issues)
	}

	if report.BackupID != stubBackupID {
		t.Errorf("BackupID = %q, want %q", report.BackupID, stubBackupID)
	}

	if report.TotalSegments != segments || report.CheckedSegments != segments ||
		report.ValidSegments != segments {
		t.Fatalf("unexpected counts: %+v", report)
	}

	if report.TotalRecords != segments {
		t.Errorf("TotalRecords = %d, want %d", report.TotalRecords, segments)
	}

	if report.TotalBytes != segments*fixtureSegmentBytes {
		t.Errorf("TotalBytes = %d, want %d", report.TotalBytes, segments*fixtureSegmentBytes)
	}

	if report.SkippedCompressed != 0 {
		t.Errorf("SkippedCompressed = %d, want 0", report.SkippedCompressed)
	}
}

func TestSegValidator_CompressedRecordsAreCounted(t *testing.T) {
	t.Parallel()

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureCompressedHex), 1)

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if report.Failed() {
		t.Fatalf("Validate() reported issues: %+v", report.Issues)
	}

	if report.SkippedCompressed != 1 || report.TotalRecords != 0 {
		t.Fatalf("compressed = %d, records = %d, want 1 and 0",
			report.SkippedCompressed, report.TotalRecords)
	}
}

func TestSegValidator_BrokenRecordIsLocated(t *testing.T) {
	t.Parallel()

	payload := decodeSegmentHex(t, fixtureSegmentHex)
	streamer := newStubStreamer(payload, 1)
	streamer.openFunc = func(string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(breakSegment(payload))), nil
	}

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if len(report.Issues) != 1 {
		t.Fatalf("Validate() issues = %+v, want exactly one", report.Issues)
	}

	issue := report.Issues[0]

	if issue.Namespace != stubNS || issue.SegmentPath != stubSegmentPath(0) {
		t.Errorf("issue points at %s/%s, want %s/%s",
			issue.Namespace, issue.SegmentPath, stubNS, stubSegmentPath(0))
	}

	if issue.RecordIndex != 0 || issue.Offset != 0 {
		t.Errorf("issue at record %d offset %d, want the first record",
			issue.RecordIndex, issue.Offset)
	}

	var recErr *segment.RecordError
	if !errors.As(issue.Err, &recErr) {
		t.Errorf("issue error = %v, want a *segment.RecordError", issue.Err)
	}

	if report.ValidSegments != 0 || report.InvalidSegments != 1 {
		t.Errorf("valid = %d, invalid = %d, want 0 and 1", report.ValidSegments, report.InvalidSegments)
	}
}

func TestSegValidator_UnreadableSegmentIsReported(t *testing.T) {
	t.Parallel()

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 1)
	streamer.openFunc = func(string) (io.ReadCloser, error) {
		return nil, errOpenSegment
	}

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if len(report.Issues) != 1 || !errors.Is(report.Issues[0].Err, errOpenSegment) {
		t.Fatalf("issues = %+v, want the open failure", report.Issues)
	}

	if report.Issues[0].RecordIndex != -1 {
		t.Errorf("RecordIndex = %d, want -1 for a segment that was never read",
			report.Issues[0].RecordIndex)
	}
}

func TestSegValidator_IssuesAreCapped(t *testing.T) {
	t.Parallel()

	const (
		segments  = 20
		maxIssues = 5
	)

	payload := decodeSegmentHex(t, fixtureSegmentHex)
	streamer := newStubStreamer(payload, segments)
	streamer.openFunc = func(string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(breakSegment(payload))), nil
	}

	report, err := newTestSegValidator(t, streamer, WithMaxIssues(maxIssues)).Validate(t.Context(), CheckAll)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if len(report.Issues) != maxIssues {
		t.Fatalf("issues = %d, want %d", len(report.Issues), maxIssues)
	}

	if report.InvalidSegments != segments || !report.Truncated() {
		t.Errorf("invalid = %d, truncated = %v, want %d and true",
			report.InvalidSegments, report.Truncated(), segments)
	}
}

func TestSegValidator_OversizedSegment(t *testing.T) {
	t.Parallel()

	streamer := newStubStreamer(nil, 1)
	streamer.openFunc = func(string) (io.ReadCloser, error) {
		return io.NopCloser(zeroReader{}), nil
	}

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if len(report.Issues) != 1 || !errors.Is(report.Issues[0].Err, ErrSegmentTooLarge) {
		t.Fatalf("issues = %+v, want the segment to be refused as too large", report.Issues)
	}
}

func TestSegValidator_SamplingDownloadsOnlyTheSample(t *testing.T) {
	t.Parallel()

	const (
		segments   = 100_000
		sampleSize = 7
	)

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), segments)

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), sampleSize)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if report.CheckedSegments != sampleSize || streamer.opened.Load() != sampleSize {
		t.Fatalf("checked %d segments and downloaded %d, want %d of each",
			report.CheckedSegments, streamer.opened.Load(), sampleSize)
	}
}

func TestSegValidator_MissingSegmentOfAManifest(t *testing.T) {
	t.Parallel()

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 2)
	streamer.fromManifest = true
	streamer.missing = map[string]bool{stubSegmentPath(1): true}

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	m := report.Manifests

	if m.MissingSegments != 1 || m.Problems != 1 || m.CheckedSegments != 2 {
		t.Fatalf("manifest report = %+v, want one missing segment out of two checked", m)
	}

	if len(m.Issues) != 1 || m.Issues[0].SegmentPath != stubSegmentPath(1) ||
		m.Issues[0].ManifestPath != stubManifestPath(1) {
		t.Fatalf("manifest issues = %+v, want the missing segment and the manifest naming it", m.Issues)
	}

	if !errors.Is(m.Issues[0].Err, streamers.ErrSegmentMissing) {
		t.Errorf("issue error = %v, want ErrSegmentMissing", m.Issues[0].Err)
	}
}

func TestSegValidator_SizeMismatchAgainstAManifest(t *testing.T) {
	t.Parallel()

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 1)
	streamer.fromManifest = true
	streamer.recordedSize = fixtureSegmentBytes * 2

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if report.Manifests.Problems != 1 || report.Manifests.MissingSegments != 0 {
		t.Fatalf("manifest report = %+v, want one problem and no missing segment", report.Manifests)
	}

	if len(report.Issues) != 1 || !errors.Is(report.Issues[0].Err, ErrSizeMismatch) {
		t.Fatalf("issues = %+v, want the size mismatch", report.Issues)
	}
}

func TestSegValidator_SizeIsOnlyCheckedAgainstAManifest(t *testing.T) {
	t.Parallel()

	// A segment found by listing carries the size the listing reported, which
	// is the size it has: there is nothing to compare it against.
	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 1)
	streamer.recordedSize = fixtureSegmentBytes * 2

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if report.Failed() {
		t.Fatalf("Validate() reported issues: %+v", report.Issues)
	}
}

func TestSegValidator_ChecksumMismatchAgainstAManifest(t *testing.T) {
	t.Parallel()

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 1)
	streamer.fromManifest = true
	streamer.recordedChecksum = "deadbeef"

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if len(report.Issues) != 1 || !errors.Is(report.Issues[0].Err, ErrChecksumMismatch) {
		t.Fatalf("issues = %+v, want the checksum mismatch", report.Issues)
	}

	if report.Manifests.Problems != 1 || len(report.Manifests.Issues) != 1 {
		t.Errorf("manifest report = %+v, want the mismatch reported against the manifest", report.Manifests)
	}
}

func TestSegValidator_ChecksumOfAReadableSegment(t *testing.T) {
	t.Parallel()

	payload := decodeSegmentHex(t, fixtureSegmentHex)
	streamer := newStubStreamer(payload, 1)
	streamer.fromManifest = true
	streamer.recordedChecksum = fmt.Sprintf("%08X", crc32.ChecksumIEEE(payload))

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	// The checksum a manifest records is hexadecimal, and a validator does not
	// care which case it was written in.
	if report.Failed() {
		t.Fatalf("Validate() reported issues: %+v, %+v", report.Issues, report.Manifests.Issues)
	}
}

func TestSegValidator_ChecksumIsOnlyCheckedWhenRecorded(t *testing.T) {
	t.Parallel()

	// A manifest that checksummed its segments some other way records nothing
	// this validator can check, and a segment that still parses is still good.
	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 1)
	streamer.fromManifest = true

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if report.Failed() {
		t.Fatalf("Validate() reported issues: %+v", report.Issues)
	}
}

func TestSegValidator_ManifestIssuesOfTheStreamerAreReported(t *testing.T) {
	t.Parallel()

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 1)
	streamer.stats = streamers.Stats{
		ManifestIssues: []streamers.ManifestIssue{{
			Err:       streamers.ErrManifestUnusable,
			Namespace: stubNS,
			Path:      stubManifestPath(0),
		}},
		ManifestsFound:  4,
		ManifestsRead:   3,
		ManifestsFailed: 1,
	}

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	m := report.Manifests

	if m.Total != 4 || m.Checked != 3 || m.Problems != 1 {
		t.Fatalf("manifest report = %+v, want 4 found, 3 read and 1 problem", m)
	}

	if len(m.Issues) != 1 || m.Issues[0].ManifestPath != stubManifestPath(0) {
		t.Fatalf("manifest issues = %+v, want the unusable manifest", m.Issues)
	}

	if !report.Failed() {
		t.Error("Failed() = false, want a backup whose manifest could not be read to fail")
	}
}

func TestSegValidator_UnrecordedSegmentsAreReported(t *testing.T) {
	t.Parallel()

	const segments = 3

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), segments)
	streamer.unrecorded = true

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	m := report.Manifests

	if m.Unrecorded != segments || len(m.UnrecordedExamples) != segments {
		t.Fatalf("manifest report = %+v, want the %d segments no manifest names", m, segments)
	}

	if m.UnrecordedExamples[0] != stubSegmentPath(0) && !slices.Contains(m.UnrecordedExamples, stubSegmentPath(0)) {
		t.Errorf("unrecorded = %v, want the streamed segments", m.UnrecordedExamples)
	}

	// They were read like any other segment, and a backup can hold them and
	// still restore, so they are reported rather than failed.
	if report.CheckedSegments != segments || report.ValidSegments != segments {
		t.Errorf("checked %d and validated %d segments, want %d of each",
			report.CheckedSegments, report.ValidSegments, segments)
	}

	if report.Failed() {
		t.Errorf("Failed() = true, want segments nothing recorded to be reported and not failed")
	}
}

func TestSegValidator_UnrecordedExamplesAreCapped(t *testing.T) {
	t.Parallel()

	const maxIssues = 4

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 20)
	streamer.unrecorded = true

	report, err := newTestSegValidator(t, streamer, WithMaxIssues(maxIssues)).Validate(t.Context(), CheckAll)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if report.Manifests.Unrecorded != 20 || len(report.Manifests.UnrecordedExamples) != maxIssues {
		t.Fatalf("manifest report = %+v, want 20 counted and %d named", report.Manifests, maxIssues)
	}
}

func TestSegValidator_NoSegments(t *testing.T) {
	t.Parallel()

	streamer := newStubStreamer(nil, 0)

	_, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	if !errors.Is(err, ErrNoSegments) {
		t.Fatalf("Validate() error = %v, want ErrNoSegments", err)
	}
}

func TestSegValidator_StreamFails(t *testing.T) {
	t.Parallel()

	streamer := newStubStreamer(nil, 0)
	streamer.streamErr = errOpenSegment

	_, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	if !errors.Is(err, errOpenSegment) {
		t.Fatalf("Validate() error = %v, want the streaming failure", err)
	}
}

func TestSegValidator_ContextCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 1_000_000)

	_, err := newTestSegValidator(t, streamer).Validate(ctx, CheckAll)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Validate() error = %v, want context.Canceled", err)
	}
}

func TestNewSegValidatorValidation(t *testing.T) {
	t.Parallel()

	if _, err := NewSegValidator(nil); err == nil {
		t.Error("NewSegValidator(nil) succeeded, want an error")
	}

	streamer := newStubStreamer(nil, 0)

	v, err := NewSegValidator(streamer, WithLogger(nil), WithParallel(0), WithMaxIssues(0))
	if err != nil {
		t.Fatalf("NewSegValidator() error = %v", err)
	}

	if v.logger == nil || v.parallel < 1 || v.maxIssues != defaultMaxIssues {
		t.Errorf("options out of range changed the defaults: %+v", v)
	}
}
