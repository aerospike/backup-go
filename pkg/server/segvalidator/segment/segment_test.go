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

package segment

import (
	"encoding/binary"
	"errors"
	"testing"
)

const (
	testSetName    = "demo"
	testBinName    = "a"
	testGeneration = uint16(1)

	testDigestSize = 20
	testSlackSize  = 8
	testLongSlack  = 128
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
	// corruptEndMark flips the end marker after it has been computed.
	corruptEndMark bool
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
func buildRecord(t *testing.T, spec recordSpec) []byte {
	t.Helper()

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
		record[digestOffset+i] = byte(i + 1)
	}

	writeLutGen(record, spec.generation, 0)

	copy(record[flatRecordHdrSize:], meta)
	copy(record[flatRecordHdrSize+len(meta):], bin)

	mark := makeEndMark(record)
	if spec.corruptEndMark {
		mark ^= 0xff
	}

	binary.LittleEndian.PutUint32(record[flatSize:], mark)

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
				return buildRecord(t, defaultSpec())
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
				return concat(buildRecord(t, defaultSpec()), buildRecord(t, otherSpec))
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
				return buildRecord(t, noBinsSpec)
			},
			wantStats: Stats{
				RecordCount: 1,
				ByteCount:   48,
			},
		},
		{
			name: "trailing zero slack",
			build: func(t *testing.T) []byte {
				t.Helper()
				return concat(buildRecord(t, defaultSpec()), make([]byte, testSlackSize))
			},
			wantStats: Stats{
				RecordCount: 1,
				ByteCount:   64,
				SlackBytes:  testSlackSize,
			},
		},
		{
			name: "zero padding longer than a record",
			build: func(t *testing.T) []byte {
				t.Helper()
				return concat(buildRecord(t, defaultSpec()), make([]byte, testLongSlack))
			},
			wantStats: Stats{
				RecordCount: 1,
				ByteCount:   64,
				SlackBytes:  testLongSlack,
			},
		},
		{
			name: "compressed record is skipped",
			build: func(t *testing.T) []byte {
				t.Helper()
				return buildRecord(t, compressedSpec)
			},
			wantStats: Stats{
				SkippedCompressed: 1,
				ByteCount:         64,
			},
		},
		{
			name: "compressed record among valid ones",
			build: func(t *testing.T) []byte {
				t.Helper()
				return concat(
					buildRecord(t, defaultSpec()),
					buildRecord(t, compressedSpec),
					buildRecord(t, otherSpec),
				)
			},
			wantStats: Stats{
				RecordCount:       2,
				SkippedCompressed: 1,
				ByteCount:         192,
			},
		},
		{
			name:    "empty payload",
			build:   func(*testing.T) []byte { return nil },
			wantErr: ErrEmptySegment,
		},
		{
			name:    "all zero payload",
			build:   func(*testing.T) []byte { return make([]byte, minRecordSize) },
			wantErr: ErrNoRecords,
		},
		{
			name: "bad magic",
			build: func(t *testing.T) []byte {
				t.Helper()
				rec := buildRecord(t, defaultSpec())
				rec[0] ^= 0xff

				return rec
			},
			wantErr: ErrBadMagic,
		},
		{
			name: "corrupted end mark",
			build: func(t *testing.T) []byte {
				t.Helper()
				return buildRecord(t, badMarkSpec)
			},
			wantErr: ErrBadEndMark,
		},
		{
			name: "corrupted digest breaks end mark",
			build: func(t *testing.T) []byte {
				t.Helper()
				rec := buildRecord(t, defaultSpec())
				rec[digestOffset] ^= 0xff

				return rec
			},
			wantErr: ErrBadEndMark,
		},
		{
			name: "non-zero tail slack",
			build: func(t *testing.T) []byte {
				t.Helper()
				return concat(buildRecord(t, defaultSpec()), []byte{0x00, 0x01, 0x00})
			},
			wantErr: ErrBadTailSlack,
		},
		{
			name: "truncated record",
			build: func(t *testing.T) []byte {
				t.Helper()
				rec := buildRecord(t, defaultSpec())

				return rec[:len(rec)-rblockSize]
			},
			wantErr: ErrRecordOutOfBounds,
		},
		{
			name: "zero generation",
			build: func(t *testing.T) []byte {
				t.Helper()
				return buildRecord(t, zeroGenSpec)
			},
			wantErr: ErrZeroGeneration,
		},
		{
			name: "zero set name length",
			build: func(t *testing.T) []byte {
				t.Helper()
				rec := buildRecord(t, defaultSpec())
				rec[flatRecordHdrSize] = 0

				return rec
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

				return rec
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
				t.Fatalf("Validate() unexpected error: %v", err)
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

	payload := concat(good, good, broken)

	_, err := Validate(payload)

	var recErr *RecordError
	if !errors.As(err, &recErr) {
		t.Fatalf("Validate() error = %v, want *RecordError", err)
	}

	if recErr.Index != 2 {
		t.Errorf("RecordError.Index = %d, want 2", recErr.Index)
	}

	if want := 2 * len(good); recErr.Offset != want {
		t.Errorf("RecordError.Offset = %d, want %d", recErr.Offset, want)
	}

	if !errors.Is(recErr, ErrBadEndMark) {
		t.Errorf("RecordError does not wrap ErrBadEndMark: %v", recErr.Err)
	}
}

func TestValidate_StatsAreCumulative(t *testing.T) {
	t.Parallel()

	const recordCount = 10

	parts := make([][]byte, 0, recordCount)
	for range recordCount {
		parts = append(parts, buildRecord(t, defaultSpec()))
	}

	payload := concat(parts...)

	stats, err := Validate(payload)
	if err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
	}

	if stats.RecordCount != recordCount {
		t.Errorf("RecordCount = %d, want %d", stats.RecordCount, recordCount)
	}

	if stats.ByteCount != len(payload) {
		t.Errorf("ByteCount = %d, want %d", stats.ByteCount, len(payload))
	}
}

func TestIsZero(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
		want bool
	}{
		{name: "nil", data: nil, want: true},
		{name: "empty", data: []byte{}, want: true},
		{name: "all zero", data: make([]byte, 64), want: true},
		{name: "leading non-zero", data: append([]byte{1}, make([]byte, 63)...), want: false},
		{name: "trailing non-zero", data: append(make([]byte, 63), 1), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := isZero(tt.data); got != tt.want {
				t.Fatalf("isZero() = %v, want %v", got, tt.want)
			}
		})
	}
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
