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

package segment

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testSetName    = "demo"
	testBinName    = "a"
	testGeneration = uint16(1)

	testDigestSize = 20
	// testPartition is the partition of every record buildRecord makes: the
	// low 12 bits of its digest, which starts 01 02 03 04.
	testPartition = 0x201

	msgValidateUnexpectedErr = "Validate() unexpected error: %v"
)

var (
	testBinValue      = []byte("hello")
	testOtherBinValue = []byte("world")
)

// recordSpec describes the record buildRecord should produce.
type recordSpec struct {
	setName    string
	binName    string
	binValue   []byte
	generation uint16
	compressed bool
	omitSet    bool
	omitBins   bool
	// digestSeed shifts every digest byte, which moves the record to another
	// partition.
	digestSeed byte
	// corruptEndMark flips the end marker after it has been computed.
	corruptEndMark bool
	// endMarkShift moves the end marker that many bytes past the content, into
	// the padding of the last rblock.
	endMarkShift int
}

// defaultSpec is a well formed single bin record.
func defaultSpec() recordSpec {
	return recordSpec{
		setName:    testSetName,
		binName:    testBinName,
		binValue:   testBinValue,
		generation: testGeneration,
	}
}

// buildRecord assembles one on-device flat record from spec.
func buildRecord(tb testing.TB, spec recordSpec) []byte {
	tb.Helper()

	var (
		meta  []byte
		bin   []byte
		flags uint32
	)

	if !spec.omitSet {
		meta = append(meta, byte(len(spec.setName)))
		meta = append(meta, spec.setName...)
		flags |= flagHasSet
	}

	if !spec.omitBins {
		meta = appendUintvar(meta, 1)
		flags |= flagHasBins

		bin = append(bin, byte(len(spec.binName)))
		bin = append(bin, spec.binName...)
		bin = append(bin, particleTypeString)
		bin = binary.LittleEndian.AppendUint32(bin, uint32(len(spec.binValue)))
		bin = append(bin, spec.binValue...)
	}

	if spec.compressed {
		flags |= flagIsCompressed
	}

	flatSize := flatRecordHdrSize + len(meta) + len(bin)
	writeSize := (flatSize + endMarkSize + rblockSize - 1) &^ (rblockSize - 1)
	record := make([]byte, writeSize)

	flags |= uint32(writeSize/rblockSize-1) & flagNRBlocksMask

	binary.LittleEndian.PutUint32(record[0:4], flatMagic)
	binary.LittleEndian.PutUint32(record[4:8], flags)

	for i := range testDigestSize {
		record[digestOffset+i] = spec.digestSeed + byte(i+1)
	}

	writeLutGen(record, spec.generation, 0)

	copy(record[flatRecordHdrSize:], meta)
	copy(record[flatRecordHdrSize+len(meta):], bin)

	mark := makeEndMark(record)
	if spec.corruptEndMark {
		mark ^= 0xff
	}

	require.LessOrEqual(tb, flatSize+spec.endMarkShift+endMarkSize, writeSize,
		"end marker shifted out of the record")
	binary.LittleEndian.PutUint32(record[flatSize+spec.endMarkShift:], mark)

	return record
}

// appendUintvar encodes val the way the server does.
func appendUintvar(buf []byte, val uint32) []byte {
	if val&0xffffff80 == 0 {
		return append(buf, byte(val))
	}

	if val&0xffffc000 == 0 {
		return append(buf, byte(val>>7)|0x80, byte(val&0x7f))
	}

	for i := 4; i > 0; i-- {
		if v := val >> uint32(7*i); v != 0 {
			buf = append(buf, byte(v)|0x80)
		}
	}

	return append(buf, byte(val&0x7f))
}

// concat joins segments into one payload.
func concat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}

	return out
}

func TestValidate(t *testing.T) {
	t.Parallel()

	noBinsSpec := defaultSpec()
	noBinsSpec.omitBins = true

	compressedSpec := defaultSpec()
	compressedSpec.compressed = true

	zeroGenSpec := defaultSpec()
	zeroGenSpec.generation = 0

	otherSpec := defaultSpec()
	otherSpec.binValue = testOtherBinValue

	badMarkSpec := defaultSpec()
	badMarkSpec.corruptEndMark = true

	shiftedMarkSpec := defaultSpec()
	shiftedMarkSpec.endMarkShift = 1

	tests := []struct {
		build     func(t *testing.T) []byte
		wantErr   error
		name      string
		wantStats Stats
	}{
		{
			name: "single record",
			build: func(t *testing.T) []byte {
				t.Helper()
				return buildSegment(t, buildRecord(t, defaultSpec()))
			},
			wantStats: Stats{
				RecordCount: 1,
				ByteCount:   64,
			},
		},
		{
			name: "two records",
			build: func(t *testing.T) []byte {
				t.Helper()
				return buildSegment(t, buildRecord(t, defaultSpec()), buildRecord(t, otherSpec))
			},
			wantStats: Stats{
				RecordCount: 2,
				ByteCount:   128,
			},
		},
		{
			name: "record without bins",
			build: func(t *testing.T) []byte {
				t.Helper()
				return buildSegment(t, buildRecord(t, noBinsSpec))
			},
			wantStats: Stats{
				RecordCount: 1,
				ByteCount:   48,
			},
		},
		{
			name: "compressed record",
			build: func(t *testing.T) []byte {
				t.Helper()
				return buildSegment(t, buildRecord(t, compressedSpec))
			},
			wantErr: ErrCompressedRecord,
		},
		{
			name: "compressed record among valid ones",
			build: func(t *testing.T) []byte {
				t.Helper()
				return buildSegment(t,
					buildRecord(t, defaultSpec()),
					buildRecord(t, compressedSpec),
					buildRecord(t, otherSpec),
				)
			},
			wantErr: ErrCompressedRecord,
		},
		{
			name:    "empty payload",
			build:   func(*testing.T) []byte { return nil },
			wantErr: ErrFrameTruncated,
		},
		{
			// What a segment written before the frame was introduced looks like.
			name: "unframed record",
			build: func(t *testing.T) []byte {
				t.Helper()
				return buildRecord(t, defaultSpec())
			},
			wantErr: ErrBadFrameMagic,
		},
		{
			name: "broken frame",
			build: func(t *testing.T) []byte {
				t.Helper()
				seg := buildSegment(t, buildRecord(t, defaultSpec()))
				binary.LittleEndian.PutUint16(seg[frameVersionMajorOffset:], frameVersionMajor+1)

				return seg
			},
			wantErr: ErrUnsupportedFrameVersion,
		},
		{
			// The server writes no padding, so zeros where a record should
			// start are corruption rather than slack.
			name: "zero rblocks after the last record",
			build: func(t *testing.T) []byte {
				t.Helper()
				return frameBody(t, concat(buildRecord(t, defaultSpec()), make([]byte, minRecordSize)), 1)
			},
			wantErr: ErrBadMagic,
		},
		{
			name: "all zero body",
			build: func(t *testing.T) []byte {
				t.Helper()
				return frameBody(t, make([]byte, minRecordSize), 0)
			},
			wantErr: ErrBadMagic,
		},
		{
			name: "residue too short for a record",
			build: func(t *testing.T) []byte {
				t.Helper()
				return frameBody(t, concat(buildRecord(t, defaultSpec()), make([]byte, rblockSize)), 1)
			},
			wantErr: ErrTruncatedRecord,
		},
		{
			name: "footer counts more records than the body holds",
			build: func(t *testing.T) []byte {
				t.Helper()
				return frameBody(t, buildRecord(t, defaultSpec()), 2)
			},
			wantErr: ErrRecordCountMismatch,
		},
		{
			name: "footer counts fewer records than the body holds",
			build: func(t *testing.T) []byte {
				t.Helper()
				return frameBody(t, concat(buildRecord(t, defaultSpec()), buildRecord(t, otherSpec)), 1)
			},
			wantErr: ErrRecordCountMismatch,
		},
		{
			name: "bad magic",
			build: func(t *testing.T) []byte {
				t.Helper()
				rec := buildRecord(t, defaultSpec())
				rec[0] ^= 0xff

				return buildSegment(t, rec)
			},
			wantErr: ErrBadMagic,
		},
		{
			// The server finds no marker in the last rblock, so it cannot trust
			// the declared size as the stride of its walk.
			name: "corrupted end mark",
			build: func(t *testing.T) []byte {
				t.Helper()
				return buildSegment(t, buildRecord(t, badMarkSpec))
			},
			wantErr: ErrFrameContentMismatch,
		},
		{
			name: "corrupted digest breaks end mark",
			build: func(t *testing.T) []byte {
				t.Helper()
				rec := buildRecord(t, defaultSpec())
				rec[digestOffset] ^= 0xff

				return buildSegment(t, rec)
			},
			wantErr: ErrFrameContentMismatch,
		},
		{
			// The marker passes the frame test, but sits where the content
			// does not end.
			name: "end mark past the content",
			build: func(t *testing.T) []byte {
				t.Helper()
				return buildSegment(t, buildRecord(t, shiftedMarkSpec))
			},
			wantErr: ErrBadEndMark,
		},
		{
			// A record claiming a spare rblock puts its marker before the last
			// one, where the server does not look for it.
			name: "record declaring one rblock too many",
			build: func(t *testing.T) []byte {
				t.Helper()
				rec := buildRecord(t, defaultSpec())
				rec = append(rec, make([]byte, rblockSize)...)
				flags := binary.LittleEndian.Uint32(rec[4:8]) + 1
				binary.LittleEndian.PutUint32(rec[4:8], flags)

				return buildSegment(t, rec)
			},
			wantErr: ErrFrameContentMismatch,
		},
		{
			// The record claims more bytes than the body has left, even though
			// the footer bytes would cover them.
			name: "record runs into the footer",
			build: func(t *testing.T) []byte {
				t.Helper()
				rec := buildRecord(t, defaultSpec())

				return buildSegment(t, rec[:len(rec)-rblockSize])
			},
			wantErr: ErrRecordOutOfBounds,
		},
		{
			// A header claiming fewer rblocks than a header itself needs
			// describes a record no record can be.
			name: "record smaller than the minimum",
			build: func(t *testing.T) []byte {
				t.Helper()
				rec := buildRecord(t, defaultSpec())
				flags := binary.LittleEndian.Uint32(rec[4:8])
				binary.LittleEndian.PutUint32(rec[4:8], (flags&^flagNRBlocksMask)|1)

				return buildSegment(t, rec)
			},
			wantErr: ErrRecordTooSmall,
		},
		{
			name: "zero generation",
			build: func(t *testing.T) []byte {
				t.Helper()
				return buildSegment(t, buildRecord(t, zeroGenSpec))
			},
			wantErr: ErrZeroGeneration,
		},
		{
			name: "zero set name length",
			build: func(t *testing.T) []byte {
				t.Helper()
				rec := buildRecord(t, defaultSpec())
				rec[flatRecordHdrSize] = 0

				return buildSegment(t, rec)
			},
			wantErr: ErrBadSetNameLength,
		},
		{
			name: "unknown particle type",
			build: func(t *testing.T) []byte {
				t.Helper()
				rec := buildRecord(t, defaultSpec())
				// header + set length + set name + n-bins + bin name length + bin name
				rec[flatRecordHdrSize+1+len(testSetName)+1+1+len(testBinName)] = 0x7f

				return buildSegment(t, rec)
			},
			wantErr: ErrUnknownParticleType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stats, err := Validate(tt.build(t))

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Validate() error = %v, want %v", err, tt.wantErr)
				}

				return
			}

			if err != nil {
				t.Fatalf(msgValidateUnexpectedErr, err)
			}

			if stats != tt.wantStats {
				t.Fatalf("Validate() stats = %+v, want %+v", stats, tt.wantStats)
			}
		})
	}
}

func TestValidate_RecordErrorPosition(t *testing.T) {
	t.Parallel()

	good := buildRecord(t, defaultSpec())

	broken := buildRecord(t, defaultSpec())
	broken[digestOffset] ^= 0xff

	_, err := Validate(buildSegment(t, good, good, broken))

	var recErr *RecordError
	if !errors.As(err, &recErr) {
		t.Fatalf("Validate() error = %v, want *RecordError", err)
	}

	if recErr.Index != 2 {
		t.Errorf("RecordError.Index = %d, want 2", recErr.Index)
	}

	// Offsets count from the start of the segment object, frame included.
	if want := frameHeaderSize + 2*len(good); recErr.Offset != want {
		t.Errorf("RecordError.Offset = %d, want %d", recErr.Offset, want)
	}

	if !errors.Is(recErr, ErrFrameContentMismatch) {
		t.Errorf("RecordError does not wrap ErrFrameContentMismatch: %v", recErr.Err)
	}
}

func TestValidate_ExtendedHeader(t *testing.T) {
	t.Parallel()

	const extendedHeaderSize = frameHeaderSize + rblockSize

	good := buildRecord(t, defaultSpec())

	broken := buildRecord(t, defaultSpec())
	broken[digestOffset] ^= 0xff

	tests := []struct {
		wantErr    error
		name       string
		give       [][]byte
		wantStats  Stats
		wantOffset int
	}{
		{
			name:      "records start after the declared header",
			give:      [][]byte{good, good},
			wantStats: Stats{RecordCount: 2, ByteCount: 2 * len(good)},
		},
		{
			name:       "broken record is located past the declared header",
			give:       [][]byte{good, broken},
			wantErr:    ErrFrameContentMismatch,
			wantOffset: extendedHeaderSize + len(good),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			seg := buildFrame(t, frameSpec{
				body:        concat(tt.give...),
				recordCount: uint64(len(tt.give)),
				headerLen:   extendedHeaderSize,
				footerLen:   frameFooterSize,
			})

			stats, err := Validate(seg)
			if tt.wantErr != nil {
				var recErr *RecordError
				require.ErrorAs(t, err, &recErr)
				require.ErrorIs(t, err, tt.wantErr)
				assert.Equal(t, tt.wantOffset, recErr.Offset)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantStats, stats)
		})
	}
}

func TestValidate_Partition(t *testing.T) {
	t.Parallel()

	otherSpec := defaultSpec()
	otherSpec.binValue = testOtherBinValue

	foreignSpec := defaultSpec()
	foreignSpec.digestSeed = 1

	brokenForeignSpec := foreignSpec
	brokenForeignSpec.corruptEndMark = true

	seg := buildSegment(t, buildRecord(t, defaultSpec()), buildRecord(t, otherSpec))
	mixed := buildSegment(t, buildRecord(t, defaultSpec()), buildRecord(t, foreignSpec))
	brokenMixed := buildSegment(t, buildRecord(t, defaultSpec()), buildRecord(t, brokenForeignSpec))

	tests := []struct {
		wantErr   error
		name      string
		give      []byte
		giveOpts  []Option
		wantStats Stats
	}{
		{
			name:      "every record in the expected partition",
			give:      seg,
			giveOpts:  []Option{WithPartition(testPartition)},
			wantStats: Stats{RecordCount: 2, ByteCount: 128},
		},
		{
			name:      "partition not checked without the option",
			give:      mixed,
			wantStats: Stats{RecordCount: 2, ByteCount: 128},
		},
		{
			name:     "segment of another partition",
			give:     seg,
			giveOpts: []Option{WithPartition(testPartition + 1)},
			wantErr:  ErrWrongPartition,
		},
		{
			name:     "one record of another partition",
			give:     mixed,
			giveOpts: []Option{WithPartition(testPartition)},
			wantErr:  ErrWrongPartition,
		},
		{
			// The server tests the frame of a record before its partition.
			name:     "broken frame reported before another partition",
			give:     brokenMixed,
			giveOpts: []Option{WithPartition(testPartition)},
			wantErr:  ErrFrameContentMismatch,
		},
		{
			name:     "negative partition",
			give:     seg,
			giveOpts: []Option{WithPartition(-1)},
			wantErr:  ErrInvalidPartition,
		},
		{
			name:     "partition past the last one",
			give:     seg,
			giveOpts: []Option{WithPartition(PartitionCount)},
			wantErr:  ErrInvalidPartition,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stats, err := Validate(tt.give, tt.giveOpts...)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantStats, stats)
		})
	}
}

func TestRecordPartition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		digest []byte
		want   int
	}{
		{name: "low 12 bits of the first word", digest: []byte{0x01, 0x02, 0x03, 0x04}, want: 0x201},
		{name: "high bits are ignored", digest: []byte{0xff, 0xff, 0xff, 0xff}, want: 0xfff},
		{name: "first partition", digest: []byte{0x00, 0xf0, 0xff, 0xff}, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := make([]byte, flatRecordHdrSize)
			copy(rec[digestOffset:], tt.digest)

			assert.Equal(t, tt.want, recordPartition(rec))
		})
	}
}

func TestValidate_FrameErrorIsNotARecordError(t *testing.T) {
	t.Parallel()

	_, err := Validate(buildRecord(t, defaultSpec()))

	var recErr *RecordError
	if errors.As(err, &recErr) {
		t.Fatalf("Validate() error = %v, want a frame error not tied to a record", err)
	}
}

func TestValidate_StatsAreCumulative(t *testing.T) {
	t.Parallel()

	const recordCount = 10

	parts := make([][]byte, 0, recordCount)
	for range recordCount {
		parts = append(parts, buildRecord(t, defaultSpec()))
	}

	body := concat(parts...)

	stats, err := Validate(buildSegment(t, parts...))
	if err != nil {
		t.Fatalf(msgValidateUnexpectedErr, err)
	}

	if stats.RecordCount != recordCount {
		t.Errorf("RecordCount = %d, want %d", stats.RecordCount, recordCount)
	}

	if stats.ByteCount != len(body) {
		t.Errorf("ByteCount = %d, want %d", stats.ByteCount, len(body))
	}
}

// FuzzValidate feeds arbitrary objects to the validator. A segment comes from
// storage nobody vouches for, so no input may panic, and whatever is accepted
// must be consistent with the object it came from.
func FuzzValidate(f *testing.F) {
	record := buildRecord(f, defaultSpec())
	partition := uint16(recordPartition(record))

	f.Add([]byte(nil), uint16(0), false)
	f.Add(record, partition, true)
	f.Add(buildSegment(f, record), partition, true)
	f.Add(buildSegment(f, record), partition+1, true)
	f.Add(buildSegment(f, record, record), partition, false)
	f.Add(buildFrame(f, frameSpec{
		body:        record,
		recordCount: 1,
		headerLen:   frameHeaderSize + rblockSize,
		footerLen:   frameFooterSize * 2,
	}), partition, true)

	f.Fuzz(func(t *testing.T, data []byte, partition uint16, checkPartition bool) {
		var opts []Option
		if checkPartition {
			opts = append(opts, WithPartition(int(partition&partitionMask)))
		}

		stats, err := Validate(data, opts...)
		require.NotErrorIs(t, err, ErrInvalidPartition)

		var recErr *RecordError
		if errors.As(err, &recErr) {
			require.GreaterOrEqual(t, recErr.Offset, frameHeaderSize)
			require.Less(t, recErr.Offset, len(data))
		}

		if err != nil {
			return
		}

		require.LessOrEqual(t, stats.ByteCount+frameOverhead, len(data))
		require.Positive(t, stats.RecordCount)
	})
}

func TestReadUintvar(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		data     []byte
		wantErr  error
		wantVal  uint32
		wantNext int
	}{
		{name: "single byte", data: []byte{0x01}, wantVal: 1, wantNext: 1},
		{name: "max single byte", data: []byte{0x7f}, wantVal: 127, wantNext: 1},
		// Most significant group first, so 0x81 0x00 is 128, not 1.
		{name: "two bytes", data: []byte{0x81, 0x00}, wantVal: 128, wantNext: 2},
		{name: "two bytes mid range", data: []byte{0x81, 0x48}, wantVal: 200, wantNext: 2},
		{name: "max two bytes", data: []byte{0xff, 0x7f}, wantVal: 16383, wantNext: 2},
		{name: "three bytes", data: []byte{0x81, 0x80, 0x00}, wantVal: 1 << 14, wantNext: 3},
		{name: "four bytes", data: []byte{0x81, 0x80, 0x80, 0x00}, wantVal: 1 << 21, wantNext: 4},
		{name: "trailing bytes ignored", data: []byte{0x81, 0x48, 0xaa}, wantVal: 200, wantNext: 2},
		{name: "leading zero", data: []byte{0x80}, wantErr: ErrLeadingZeroUvar},
		{name: "truncated", data: []byte{0x81}, wantErr: ErrTruncatedUintvar},
		{name: "truncated multi byte", data: []byte{0x81, 0x80}, wantErr: ErrTruncatedUintvar},
		{name: "empty", data: nil, wantErr: ErrTruncatedUintvar},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			val, next, err := readUintvar(tt.data, 0, len(tt.data))

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("readUintvar() error = %v, want %v", err, tt.wantErr)
				}

				return
			}

			if err != nil {
				t.Fatalf("readUintvar() unexpected error: %v", err)
			}

			if val != tt.wantVal || next != tt.wantNext {
				t.Fatalf("readUintvar() = (%d, %d), want (%d, %d)", val, next, tt.wantVal, tt.wantNext)
			}
		})
	}
}
