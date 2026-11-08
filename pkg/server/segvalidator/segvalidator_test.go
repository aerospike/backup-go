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

package segvalidator

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aerospike/backup-go/pkg/server/segvalidator/models"
	"github.com/aerospike/backup-go/pkg/server/segvalidator/segment"
	"github.com/aerospike/backup-go/pkg/server/segvalidator/streamers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The ASBK v1 frame around a 64 byte body holding one record. The headers of
// the two streams differ in the block type only.
const (
	fixtureFrameHeaderHex       = "4153424b01000000010000002000000040000000000000001000000000000000"
	fixtureChangeFrameHeaderHex = "4153424b01000000020000002000000040000000000000001000000000000000"
	fixtureFrameFooterHex       = "01000000000000000000000000000000"
)

// The 64 byte flat record of the segment fixtures, holding set "demo" and a
// single string bin. Its digest starts 01 02 03 04, which places it in
// partition fixturePartition. The two variants differ in the flags word only.
const (
	fixtureRecordMagicHex      = "01f27a03"
	fixtureRecordFlagsHex      = "03005000"
	fixtureCompressedFlagsHex  = "0300d000"
	fixtureRecordDigestTailHex = "0102030405060708090a0b0c0d0e0f10111213140000" +
		"00000001000464656d6f010161030500000068656c6c6fe127ea0700000000000000"
)

// fixtureSegmentHex is a one record segment.
const fixtureSegmentHex = fixtureFrameHeaderHex +
	fixtureRecordMagicHex + fixtureRecordFlagsHex + fixtureRecordDigestTailHex +
	fixtureFrameFooterHex

// fixtureChangeSegmentHex is the same segment as the change stream writes it.
const fixtureChangeSegmentHex = fixtureChangeFrameHeaderHex +
	fixtureRecordMagicHex + fixtureRecordFlagsHex + fixtureRecordDigestTailHex +
	fixtureFrameFooterHex

// fixtureCompressedHex is the same segment with the is_compressed flag set.
const fixtureCompressedHex = fixtureFrameHeaderHex +
	fixtureRecordMagicHex + fixtureCompressedFlagsHex + fixtureRecordDigestTailHex +
	fixtureFrameFooterHex

const (
	stubBackupID = "519118324"
	stubNS       = "source-ns1"

	// fixtureSegmentBytes is the size of the segment fixtures, frame included.
	fixtureSegmentBytes = 112
	// fixtureRecordBytes is the size of the record a segment fixture holds.
	fixtureRecordBytes = 64
	// fixtureFrameHeaderBytes is where the record of a segment fixture starts.
	fixtureFrameHeaderBytes = 32
	// fixturePartition is the partition the record of the fixtures belongs to.
	fixturePartition = 0x201
	// fixtureSegmentCRC32C is the CRC-32C of fixtureSegmentHex, the checksum
	// the server records for it.
	fixtureSegmentCRC32C = "33d1f0a2"
	// fixtureSegmentCRC32 is the IEEE CRC-32 of fixtureSegmentHex.
	fixtureSegmentCRC32 = "68139db6"
)

// Messages repeated across the tests.
const (
	msgValidateIssues = "Validate() reported issues: %+v"
	msgReport         = "report = %+v"
)

var errOpenSegment = errors.New("open failed")

// stubSegmentPath names the nth segment a stubStreamer streams. Every one of
// them sits in the partition the fixture record belongs to.
func stubSegmentPath(n int) string {
	return stubQuerySegmentPath(fixturePartition, n)
}

// stubQuerySegmentPath names the nth query stream segment of a partition.
func stubQuerySegmentPath(partition, n int) string {
	return fmt.Sprintf("%s/ns/%s/query-stream/data/p%d/s%d.seg", stubBackupID, stubNS, partition, n)
}

// stubManifestPath names the manifest a stubStreamer says segment n came from.
func stubManifestPath(n int) string {
	return stubQueryManifestPath(fixturePartition, n)
}

// stubQueryManifestPath names manifest n of a partition, n standing in for the
// regime to keep the names apart.
func stubQueryManifestPath(partition, n int) string {
	return fmt.Sprintf("%s/ns/%s/query-stream/manifest/%d-%d-0000000000000-0.json",
		stubBackupID, stubNS, partition, n)
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
	// recordedChecksum is the CRC-32C the manifests claim.
	recordedChecksum string
	// recordedCount is the number of records the manifests claim.
	recordedCount int64
	// stats is what the streamer reports having seen.
	stats streamers.Stats
	// segments is the number of segments the backup holds.
	segments int
	// fromManifest makes every segment one a manifest named.
	fromManifest bool
	// unrecorded makes every segment one no manifest names.
	unrecorded bool
	// streamKind is the stream every segment belongs to.
	streamKind streamers.Stream
	// pathOf names the nth segment.
	pathOf func(n int) string
	// manifestOf names the manifest the nth segment came from, when
	// fromManifest is set.
	manifestOf func(n int) string

	opened atomic.Int64
}

func newStubStreamer(payload []byte, segments int) *stubStreamer {
	return &stubStreamer{
		payload:    payload,
		segments:   segments,
		streamKind: streamers.QueryStream,
		pathOf:     stubSegmentPath,
		manifestOf: stubManifestPath,
	}
}

func (s *stubStreamer) BackupID() string {
	return stubBackupID
}

func (s *stubStreamer) StreamAll(ctx context.Context, out chan<- streamers.Segment) error {
	return s.stream(ctx, s.segments, out)
}

func (s *stubStreamer) StreamSample(
	ctx context.Context, n int, out chan<- streamers.Segment,
) error {
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
			Stream:    s.streamKind,
			Path:      s.pathOf(i),
			Size:      int64(len(s.payload)),
		}

		seg.Unrecorded = s.unrecorded

		if s.fromManifest {
			seg.Manifest = s.manifestOf(i)
			seg.Checksum = s.recordedChecksum
			seg.RecordCount = s.recordedCount

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

func (s *stubStreamer) OpenSegment(
	_ context.Context, seg *streamers.Segment,
) (io.ReadCloser, error) {
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
	require.NoError(t, err, "decode fixture")

	return b
}

// breakSegment returns a copy of the segment with a digest byte flipped, which
// breaks the end marker of its first record.
func breakSegment(payload []byte) []byte {
	broken := bytes.Clone(payload)
	broken[fixtureFrameHeaderBytes+8] ^= 0xff

	return broken
}

func newTestSegValidator(t *testing.T, streamer Streamer, opts ...Option) *SegValidator {
	t.Helper()

	v, err := NewSegValidator(streamer, opts...)
	require.NoError(t, err)

	return v
}

func TestSegValidator_ValidateReadableBackup(t *testing.T) {
	t.Parallel()

	const segments = 3

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), segments)

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	require.NoError(t, err)
	require.False(t, report.Failed(), msgValidateIssues, report.Issues)

	assert.Equal(t, stubBackupID, report.BackupID)

	require.EqualValues(t, segments, report.TotalSegments)
	require.EqualValues(t, segments, report.CheckedSegments)
	require.EqualValues(t, segments, report.ValidSegments)

	assert.EqualValues(t, segments, report.TotalRecords)
	assert.EqualValues(t, segments*fixtureRecordBytes, report.TotalBytes)
}

func TestSegValidator_UnrestorableSegmentIsRefused(t *testing.T) {
	t.Parallel()

	otherPartitionSegment := func(n int) string {
		return stubQuerySegmentPath(fixturePartition+1, n)
	}

	tests := []struct {
		wantErr      error
		pathOf       func(n int) string
		manifestOf   func(n int) string
		name         string
		give         string
		fromManifest bool
	}{
		{
			name:       "compressed record",
			give:       fixtureCompressedHex,
			pathOf:     stubSegmentPath,
			manifestOf: stubManifestPath,
			wantErr:    segment.ErrCompressedRecord,
		},
		{
			name:       "listed record outside the partition of its directory",
			give:       fixtureSegmentHex,
			pathOf:     otherPartitionSegment,
			manifestOf: stubManifestPath,
			wantErr:    segment.ErrWrongPartition,
		},
		{
			name:         "record outside the partition of its manifest",
			give:         fixtureSegmentHex,
			fromManifest: true,
			pathOf:       stubSegmentPath,
			manifestOf: func(n int) string {
				return stubQueryManifestPath(fixturePartition+1, n)
			},
			wantErr: segment.ErrWrongPartition,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			streamer := newStubStreamer(decodeSegmentHex(t, tt.give), 1)
			streamer.fromManifest = tt.fromManifest
			// Only fixtureSegmentHex is streamed from a manifest.
			streamer.recordedChecksum = fixtureSegmentCRC32C
			streamer.pathOf = tt.pathOf
			streamer.manifestOf = tt.manifestOf

			report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
			require.NoError(t, err)

			require.Len(t, report.Issues, 1)
			require.ErrorIs(t, report.Issues[0].Err, tt.wantErr)

			assert.Zero(t, report.ValidSegments)
			assert.EqualValues(t, 1, report.InvalidSegments)
		})
	}
}

func TestSegValidator_ChangeStreamIsNotPartitionChecked(t *testing.T) {
	t.Parallel()

	// A change stream segment mixes partitions, so the directory it sits in
	// says nothing about its records.
	streamer := newStubStreamer(decodeSegmentHex(t, fixtureChangeSegmentHex), 1)
	streamer.streamKind = streamers.ChangeStream
	streamer.pathOf = func(n int) string {
		return fmt.Sprintf("%s/ns/%s/change-stream/BB951D8A16DC7A2/data/p0/s%d.seg",
			stubBackupID, stubNS, n)
	}

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	require.NoError(t, err)
	require.False(t, report.Failed(), msgValidateIssues, report.Issues)
}

func TestSegValidator_ManifestPartitionOutranksDirectory(t *testing.T) {
	t.Parallel()

	// The server restores a segment into the partition of the manifest naming
	// it, wherever the segment sits.
	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 1)
	streamer.fromManifest = true
	streamer.recordedChecksum = fixtureSegmentCRC32C
	streamer.pathOf = func(n int) string {
		return stubQuerySegmentPath(fixturePartition+1, n)
	}

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	require.NoError(t, err)
	require.False(t, report.Failed(), msgValidateIssues, report.Issues)
}

func TestSegValidator_BrokenRecordIsLocated(t *testing.T) {
	t.Parallel()

	payload := decodeSegmentHex(t, fixtureSegmentHex)
	streamer := newStubStreamer(payload, 1)
	streamer.openFunc = func(string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(breakSegment(payload))), nil
	}

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	require.NoError(t, err)
	require.Len(t, report.Issues, 1)

	issue := report.Issues[0]

	assert.Equal(t, stubNS, issue.Namespace)
	assert.Equal(t, stubSegmentPath(0), issue.SegmentPath)

	// The issue points at the first record.
	assert.Zero(t, issue.RecordIndex)
	assert.Equal(t, fixtureFrameHeaderBytes, issue.Offset)

	var recordErr *segment.RecordError
	require.ErrorAs(t, issue.Err, &recordErr)

	assert.Zero(t, report.ValidSegments)
	assert.EqualValues(t, 1, report.InvalidSegments)
}

func TestSegValidator_UnreadableSegmentIsReported(t *testing.T) {
	t.Parallel()

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 1)
	streamer.openFunc = func(string) (io.ReadCloser, error) {
		return nil, errOpenSegment
	}

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	require.NoError(t, err)

	require.Len(t, report.Issues, 1)
	require.ErrorIs(t, report.Issues[0].Err, errOpenSegment)

	assert.Equal(t, -1, report.Issues[0].RecordIndex, "want -1 for a segment that was never read")
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

	report, err := newTestSegValidator(t, streamer, WithMaxIssues(maxIssues)).
		Validate(t.Context(), CheckAll)
	require.NoError(t, err)
	require.Len(t, report.Issues, maxIssues)

	assert.EqualValues(t, segments, report.InvalidSegments)
	assert.True(t, report.Truncated())
}

func TestSegValidator_OversizedSegment(t *testing.T) {
	t.Parallel()

	streamer := newStubStreamer(nil, 1)
	streamer.openFunc = func(string) (io.ReadCloser, error) {
		return io.NopCloser(zeroReader{}), nil
	}

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	require.NoError(t, err)

	// The segment is refused as too large.
	require.Len(t, report.Issues, 1)
	require.ErrorIs(t, report.Issues[0].Err, ErrSegmentTooLarge)
}

func TestSegValidator_FullSegmentIsNotTooLarge(t *testing.T) {
	t.Parallel()

	// A full body plus its frame is the largest segment the server writes, so
	// it reaches the parser instead of being refused by its size.
	streamer := newStubStreamer(make([]byte, segment.MaxSegmentSize), 1)

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	require.NoError(t, err)

	// The segment is parsed and its frame refused.
	require.Len(t, report.Issues, 1)
	require.ErrorIs(t, report.Issues[0].Err, segment.ErrBadFrameMagic)
}

func TestSegValidator_SamplingDownloadsOnlyTheSample(t *testing.T) {
	t.Parallel()

	const (
		segments   = 100_000
		sampleSize = 7
	)

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), segments)

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), sampleSize)
	require.NoError(t, err)

	require.EqualValues(t, sampleSize, report.CheckedSegments, "checked segments")
	require.EqualValues(t, sampleSize, streamer.opened.Load(), "downloaded segments")
}

func TestSegValidator_MissingSegmentOfAManifest(t *testing.T) {
	t.Parallel()

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 2)
	streamer.fromManifest = true
	streamer.recordedChecksum = fixtureSegmentCRC32C
	streamer.missing = map[string]bool{stubSegmentPath(1): true}

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	require.NoError(t, err)

	m := report.Manifests

	// One missing segment out of two checked.
	require.EqualValues(t, 1, m.MissingSegments)
	require.EqualValues(t, 1, m.Problems)
	require.EqualValues(t, 2, m.CheckedSegments)

	// The missing segment and the manifest naming it.
	require.Len(t, m.Issues, 1)
	require.Equal(t, stubSegmentPath(1), m.Issues[0].SegmentPath)
	require.Equal(t, stubManifestPath(1), m.Issues[0].ManifestPath)

	assert.ErrorIs(t, m.Issues[0].Err, streamers.ErrSegmentMissing)
}

func TestSegValidator_SizeMismatchAgainstAManifest(t *testing.T) {
	t.Parallel()

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 1)
	streamer.fromManifest = true
	streamer.recordedSize = fixtureSegmentBytes * 2

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	require.NoError(t, err)

	require.EqualValues(t, 1, report.Manifests.Problems)
	require.Zero(t, report.Manifests.MissingSegments)

	require.Len(t, report.Issues, 1)
	require.ErrorIs(t, report.Issues[0].Err, ErrSizeMismatch)
}

func TestSegValidator_SizeIsOnlyCheckedAgainstAManifest(t *testing.T) {
	t.Parallel()

	// A segment found by listing carries the size the listing reported, which
	// is the size it has: there is nothing to compare it against.
	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 1)
	streamer.recordedSize = fixtureSegmentBytes * 2

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	require.NoError(t, err)
	require.False(t, report.Failed(), msgValidateIssues, report.Issues)
}

func TestSegValidator_SegmentAgainstItsManifest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		giveChecksum string
		giveCount    int64
		// wantErr is nil for a segment matching its manifest.
		wantErr error
	}{
		{
			name:         "matching checksum and record count",
			giveChecksum: fixtureSegmentCRC32C,
			giveCount:    1,
		},
		{
			// The checksum is hexadecimal, and its case does not matter.
			name:         "checksum in upper case",
			giveChecksum: strings.ToUpper(fixtureSegmentCRC32C),
		},
		{
			// A record count is not recorded by every manifest.
			name:         "checksum without record count",
			giveChecksum: fixtureSegmentCRC32C,
		},
		{
			// The server takes an empty checksum for a zero.
			name:    "empty checksum",
			wantErr: ErrChecksumMismatch,
		},
		{
			name:         "checksum of another segment",
			giveChecksum: "deadbeef",
			wantErr:      ErrChecksumMismatch,
		},
		{
			// The server checks CRC-32C whatever algorithm the manifest
			// declares, so a CRC-32 of the right bytes is still a mismatch.
			name:         "IEEE CRC-32 of the segment",
			giveChecksum: fixtureSegmentCRC32,
			wantErr:      ErrChecksumMismatch,
		},
		{
			name:         "more records recorded than the segment holds",
			giveChecksum: fixtureSegmentCRC32C,
			giveCount:    2,
			wantErr:      ErrRecordCountMismatch,
		},
		{
			name:         "negative record count",
			giveChecksum: fixtureSegmentCRC32C,
			giveCount:    -1,
			wantErr:      ErrRecordCountMismatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 1)
			streamer.fromManifest = true
			streamer.recordedChecksum = tt.giveChecksum
			streamer.recordedCount = tt.giveCount

			report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
			require.NoError(t, err)

			if tt.wantErr == nil {
				require.False(t, report.Failed(), "Validate() reported issues: %+v, %+v",
					report.Issues, report.Manifests.Issues)

				return
			}

			require.Len(t, report.Issues, 1)
			require.ErrorIs(t, report.Issues[0].Err, tt.wantErr)

			// The mismatch is reported against the manifest.
			assert.EqualValues(t, 1, report.Manifests.Problems)
			assert.Len(t, report.Manifests.Issues, 1)
		})
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
	require.NoError(t, err)

	m := report.Manifests

	require.EqualValues(t, 4, m.Total)
	require.EqualValues(t, 3, m.Checked)
	require.EqualValues(t, 1, m.Problems)

	// The unusable manifest.
	require.Len(t, m.Issues, 1)
	require.Equal(t, stubManifestPath(0), m.Issues[0].ManifestPath)

	assert.True(t, report.Failed(), "want a backup whose manifest could not be read to fail")
}

func TestSegValidator_UnrecordedSegmentsAreReported(t *testing.T) {
	t.Parallel()

	const segments = 3

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), segments)
	streamer.unrecorded = true

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	require.NoError(t, err)

	m := report.Manifests

	// The segments no manifest names.
	require.EqualValues(t, segments, m.Unrecorded)
	require.Len(t, m.UnrecordedExamples, segments)

	assert.Contains(t, m.UnrecordedExamples, stubSegmentPath(0))

	// They were read like any other segment, and a backup can hold them and
	// still restore, so they are reported rather than failed.
	assert.EqualValues(t, segments, report.CheckedSegments)
	assert.EqualValues(t, segments, report.ValidSegments)

	assert.False(t, report.Failed(), "want segments nothing recorded to be reported and not failed")
}

func TestSegValidator_UnrecordedExamplesAreCapped(t *testing.T) {
	t.Parallel()

	const maxIssues = 4

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 20)
	streamer.unrecorded = true

	report, err := newTestSegValidator(t, streamer, WithMaxIssues(maxIssues)).
		Validate(t.Context(), CheckAll)
	require.NoError(t, err)

	// 20 counted and maxIssues named.
	require.EqualValues(t, 20, report.Manifests.Unrecorded)
	require.Len(t, report.Manifests.UnrecordedExamples, maxIssues)
}

func TestSegValidator_NoSegments(t *testing.T) {
	t.Parallel()

	streamer := newStubStreamer(nil, 0)

	_, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	require.ErrorIs(t, err, ErrNoSegments)
}

func TestSegValidator_BackupOfANamespaceThatHeldNoRecords(t *testing.T) {
	t.Parallel()

	// The manifests of the backup were read and recorded no segment, which is
	// what backing up an empty namespace writes. There is nothing wrong with
	// it, and the run says so instead of refusing to report on it.
	streamer := newStubStreamer(nil, 0)
	streamer.stats = streamers.Stats{Namespaces: 1, ManifestsFound: 4096, ManifestsRead: 4096}

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), 10_000)
	require.NoError(t, err)

	assert.False(t, report.Failed(), msgReport, report)

	// No segment checked and every manifest read.
	assert.Zero(t, report.CheckedSegments)
	assert.EqualValues(t, 4096, report.Manifests.Checked)
}

// TestSegValidator_EmptyNamespaceOnDisk runs a validator over a whole backup of
// a namespace that held no records, rather than over a stub: nothing was
// written but the manifests, each of them recording no segment.
func TestSegValidator_EmptyNamespaceOnDisk(t *testing.T) {
	t.Parallel()

	const (
		backupID   = "527139336"
		namespace  = "test"
		partitions = 8
	)

	root := t.TempDir()
	manifests := filepath.Join(root, backupID, "ns", namespace, "query-stream", "manifest")

	require.NoError(t, os.MkdirAll(manifests, 0o750), "create manifest directory")

	for p := range partitions {
		body := fmt.Sprintf(`{"backup_id":%q,"namespace":%q,"partition_id":%d,"format_version":1,`+
			`"checksum_algorithm":"crc32c","entry_count":0,"segments":[],"partition_complete":true}`,
			backupID, namespace, p)

		name := filepath.Join(manifests, fmt.Sprintf("%d-7-0000181197010-%06x.json", p, p))
		require.NoError(t, os.WriteFile(name, []byte(body), 0o600), "write manifest")
	}

	metadata := `{"backup_id":"` + backupID + `","namespace":"` + namespace +
		`","status":"complete","achieved_consistency":"strict"}`
	require.NoError(t, os.WriteFile(filepath.Join(root, backupID, "metadata.json"), []byte(metadata), 0o600),
		"write commit record")

	streamer, err := streamers.NewLocal(root, backupID)
	require.NoError(t, err)

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), 10_000)
	require.NoError(t, err)

	// A backup holding no segment passes.
	require.False(t, report.Failed(), msgReport, report)
	require.Zero(t, report.CheckedSegments)

	assert.EqualValues(t, partitions, report.Manifests.Checked)

	// A backup id nothing was written under is still the one thing there is
	// nothing to report about.
	missing, err := streamers.NewLocal(root, "nosuchbackup")
	require.NoError(t, err)

	_, err = newTestSegValidator(t, missing).Validate(t.Context(), 10_000)
	require.ErrorIs(t, err, ErrNoSegments)
}

func TestSegValidator_NoSegmentsButUnreadableManifests(t *testing.T) {
	t.Parallel()

	// A backup whose manifests are all unreadable and whose data directories
	// hold nothing is a broken backup, not a missing one, and what the run
	// found out about it must not be thrown away with an error.
	streamer := newStubStreamer(nil, 0)
	streamer.stats = streamers.Stats{
		ManifestIssues: []streamers.ManifestIssue{{
			Err:       streamers.ErrManifestUnusable,
			Namespace: stubNS,
			Path:      stubManifestPath(0),
		}},
		ManifestsFound:  1,
		ManifestsFailed: 1,
	}

	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	require.NoError(t, err)

	// No segment checked and the unreadable manifest reported.
	require.Zero(t, report.CheckedSegments)
	require.EqualValues(t, 1, report.Manifests.Problems)

	assert.True(t, report.Failed(), "want a backup whose manifests cannot be read to fail")
}

func TestSegValidator_StreamFails(t *testing.T) {
	t.Parallel()

	streamer := newStubStreamer(nil, 0)
	streamer.streamErr = errOpenSegment

	_, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	require.ErrorIs(t, err, errOpenSegment)
}

func TestSegValidator_ContextCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 1_000_000)

	_, err := newTestSegValidator(t, streamer).Validate(ctx, CheckAll)
	require.ErrorIs(t, err, context.Canceled)
}

func TestNewSegValidatorValidation(t *testing.T) {
	t.Parallel()

	_, err := NewSegValidator(nil)
	require.Error(t, err)

	streamer := newStubStreamer(nil, 0)

	v, err := NewSegValidator(streamer, WithLogger(nil), WithParallel(0), WithMaxIssues(0))
	require.NoError(t, err)

	// Options out of range do not change the defaults.
	assert.NotNil(t, v.logger)
	assert.GreaterOrEqual(t, v.parallel, 1)
	assert.Equal(t, defaultMaxIssues, v.maxIssues)
}

// errReader fails partway through a segment, which is what a storage that
// stops answering in the middle of a download looks like.
type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) {
	return 0, r.err
}

func TestSegValidator_SegmentReadFails(t *testing.T) {
	t.Parallel()

	errRead := errors.New("connection reset")

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 1)
	streamer.openFunc = func(string) (io.ReadCloser, error) {
		return io.NopCloser(errReader{err: errRead}), nil
	}

	// A download that dies halfway is what is wrong with that segment, not a
	// reason to abandon the run.
	report, err := newTestSegValidator(t, streamer).Validate(t.Context(), CheckAll)
	require.NoError(t, err)

	require.Len(t, report.Issues, 1)
	require.ErrorIs(t, report.Issues[0].Err, errRead)

	// Nothing was parsed, so the failure points at no record in particular.
	assert.Equal(t, models.UnknownRecordIndex, report.Issues[0].RecordIndex)

	assert.EqualValues(t, 1, report.InvalidSegments)
	assert.Zero(t, report.ValidSegments)
}

func TestSegValidator_StreamerManifestIssuesAreCapped(t *testing.T) {
	t.Parallel()

	const maxIssues = 1

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 1)
	streamer.stats = streamers.Stats{
		ManifestIssues: []streamers.ManifestIssue{
			{Err: streamers.ErrManifestUnusable, Namespace: stubNS, Path: stubManifestPath(0)},
			{Err: streamers.ErrManifestUnusable, Namespace: stubNS, Path: stubManifestPath(1)},
		},
		ManifestsFound:  2,
		ManifestsFailed: 2,
	}

	report, err := newTestSegValidator(t, streamer, WithMaxIssues(maxIssues)).
		Validate(t.Context(), CheckAll)
	require.NoError(t, err)

	m := report.Manifests

	// maxIssues problems described and 2 counted.
	require.Len(t, m.Issues, maxIssues)
	require.EqualValues(t, 2, m.Problems)

	assert.True(t, m.Truncated(), "want a report describing fewer problems than it counted")
}

func TestNewSegValidator_Options(t *testing.T) {
	t.Parallel()

	const (
		parallel  = 3
		maxIssues = 7
	)

	logger := slog.New(slog.DiscardHandler)

	v, err := NewSegValidator(newStubStreamer(nil, 0),
		WithLogger(logger), WithParallel(parallel), WithMaxIssues(maxIssues))
	require.NoError(t, err)

	assert.Same(t, logger, v.logger, "WithLogger() did not set the logger")
	assert.Equal(t, parallel, v.parallel)
	assert.Equal(t, maxIssues, v.maxIssues)
}

func TestSegValidator_ContextCancelledMidRun(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	streamer := newStubStreamer(decodeSegmentHex(t, fixtureSegmentHex), 1_000_000)
	// A run canceled once it is under way stops where it is instead of
	// walking the rest of the backup.
	streamer.openFunc = func(string) (io.ReadCloser, error) {
		cancel()

		return io.NopCloser(bytes.NewReader(streamer.payload)), nil
	}

	_, err := newTestSegValidator(t, streamer, WithParallel(1)).Validate(ctx, CheckAll)
	require.ErrorIs(t, err, context.Canceled)

	assert.LessOrEqual(t, streamer.opened.Load(), int64(1_000),
		"downloaded segments after the run was canceled, want it to stop")
}
