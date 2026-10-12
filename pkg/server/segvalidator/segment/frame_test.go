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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testVersionMinorOffset locates a frame field the parser never reads.
const testVersionMinorOffset = 6

// frameSpec describes the frame buildFrame should wrap around a body.
type frameSpec struct {
	body        []byte
	recordCount uint64
	headerLen   int
	footerLen   int
}

// buildFrame assembles a segment object the way the server writes it.
func buildFrame(tb testing.TB, spec frameSpec) []byte {
	tb.Helper()

	seg := make([]byte, spec.headerLen+len(spec.body)+spec.footerLen)

	copy(seg, frameMagic)
	binary.LittleEndian.PutUint16(seg[frameVersionMajorOffset:], frameVersionMajor)
	binary.LittleEndian.PutUint16(seg[frameBlockTypeOffset:], frameBlockRecords)
	binary.LittleEndian.PutUint32(seg[frameHeaderLenOffset:], uint32(spec.headerLen))
	binary.LittleEndian.PutUint64(seg[frameBodyLenOffset:], uint64(len(spec.body)))
	binary.LittleEndian.PutUint32(seg[frameFooterLenOffset:], uint32(spec.footerLen))

	copy(seg[spec.headerLen:], spec.body)
	binary.LittleEndian.PutUint64(seg[spec.headerLen+len(spec.body):], spec.recordCount)

	return seg
}

// frameBody frames body with the current header and footer sizes and a footer
// that claims recordCount records.
func frameBody(tb testing.TB, body []byte, recordCount uint64) []byte {
	tb.Helper()

	return buildFrame(tb, frameSpec{
		body:        body,
		recordCount: recordCount,
		headerLen:   frameHeaderSize,
		footerLen:   frameFooterSize,
	})
}

// buildSegment frames records with a footer that counts them all.
func buildSegment(tb testing.TB, records ...[]byte) []byte {
	tb.Helper()

	return frameBody(tb, concat(records...), uint64(len(records)))
}

func TestParseFrame(t *testing.T) {
	t.Parallel()

	const (
		extendedHeaderSize = frameHeaderSize + rblockSize
		extendedFooterSize = frameFooterSize * 2
		misalignedHeader   = frameHeaderSize + 1
		ancillaryFlag      = 0x0100
		criticalFlag       = 0x0001
		manifestBlockType  = 3
		minorVersion       = 7
	)

	record := buildRecord(t, defaultSpec())

	// oneRecordSegment returns a fresh one record segment for a case to patch.
	oneRecordSegment := func(t *testing.T) []byte {
		t.Helper()
		return buildSegment(t, record)
	}

	oneRecord := frame{recordCount: 1, bodyOff: frameHeaderSize, bodyLen: len(record)}

	tests := []struct {
		build   func(t *testing.T) []byte
		wantErr error
		name    string
		want    frame
	}{
		{
			name:  "current frame",
			build: oneRecordSegment,
			want:  oneRecord,
		},
		{
			name: "longer header and footer are honored",
			build: func(t *testing.T) []byte {
				t.Helper()
				return buildFrame(t, frameSpec{
					body:        record,
					recordCount: 1,
					headerLen:   extendedHeaderSize,
					footerLen:   extendedFooterSize,
				})
			},
			want: frame{recordCount: 1, bodyOff: extendedHeaderSize, bodyLen: len(record)},
		},
		{
			name: "bytes after the footer are ignored",
			build: func(t *testing.T) []byte {
				t.Helper()
				return concat(oneRecordSegment(t), make([]byte, rblockSize))
			},
			want: oneRecord,
		},
		{
			name: "unknown ancillary flag is ignored",
			build: func(t *testing.T) []byte {
				t.Helper()
				seg := oneRecordSegment(t)
				binary.LittleEndian.PutUint16(seg[frameFlagsOffset:], ancillaryFlag)

				return seg
			},
			want: oneRecord,
		},
		{
			name: "minor version is ignored",
			build: func(t *testing.T) []byte {
				t.Helper()
				seg := oneRecordSegment(t)
				binary.LittleEndian.PutUint16(seg[testVersionMinorOffset:], minorVersion)

				return seg
			},
			want: oneRecord,
		},
		{
			name: "shorter than the version prefix",
			build: func(t *testing.T) []byte {
				t.Helper()
				return oneRecordSegment(t)[:framePrefixSize-1]
			},
			wantErr: ErrFrameTruncated,
		},
		{
			name: "shorter than the header",
			build: func(t *testing.T) []byte {
				t.Helper()
				return oneRecordSegment(t)[:frameHeaderSize-1]
			},
			wantErr: ErrFrameTruncated,
		},
		{
			name: "bad magic",
			build: func(t *testing.T) []byte {
				t.Helper()
				seg := oneRecordSegment(t)
				seg[0] ^= 0xff

				return seg
			},
			wantErr: ErrBadFrameMagic,
		},
		{
			name: "flat record without a frame",
			build: func(*testing.T) []byte {
				return record
			},
			wantErr: ErrBadFrameMagic,
		},
		{
			name: "newer major version",
			build: func(t *testing.T) []byte {
				t.Helper()
				seg := oneRecordSegment(t)
				binary.LittleEndian.PutUint16(seg[frameVersionMajorOffset:], frameVersionMajor+1)

				return seg
			},
			wantErr: ErrUnsupportedFrameVersion,
		},
		{
			name: "unknown critical flag",
			build: func(t *testing.T) []byte {
				t.Helper()
				seg := oneRecordSegment(t)
				binary.LittleEndian.PutUint16(seg[frameFlagsOffset:], criticalFlag)

				return seg
			},
			wantErr: ErrUnsupportedFrameFlags,
		},
		{
			name: "reserved block type",
			build: func(t *testing.T) []byte {
				t.Helper()
				seg := oneRecordSegment(t)
				binary.LittleEndian.PutUint16(seg[frameBlockTypeOffset:], manifestBlockType)

				return seg
			},
			wantErr: ErrUnsupportedBlockType,
		},
		{
			name: "header length below the minimum",
			build: func(t *testing.T) []byte {
				t.Helper()
				seg := oneRecordSegment(t)
				binary.LittleEndian.PutUint32(seg[frameHeaderLenOffset:], frameHeaderSize-1)

				return seg
			},
			wantErr: ErrBadFrameHeaderLength,
		},
		{
			name: "footer length below the minimum",
			build: func(t *testing.T) []byte {
				t.Helper()
				seg := oneRecordSegment(t)
				binary.LittleEndian.PutUint32(seg[frameFooterLenOffset:], frameFooterSize-1)

				return seg
			},
			wantErr: ErrBadFrameFooterLength,
		},
		{
			name: "body length wider than 32 bits",
			build: func(t *testing.T) []byte {
				t.Helper()
				seg := oneRecordSegment(t)
				binary.LittleEndian.PutUint64(seg[frameBodyLenOffset:], math.MaxUint32+1)

				return seg
			},
			wantErr: ErrBadBodyLength,
		},
		{
			// header + body + footer is exactly zero modulo 2^64.
			name: "body length wrapping the frame size",
			build: func(t *testing.T) []byte {
				t.Helper()
				seg := oneRecordSegment(t)
				binary.LittleEndian.PutUint64(seg[frameBodyLenOffset:], math.MaxUint64-frameOverhead+1)

				return seg
			},
			wantErr: ErrBadBodyLength,
		},
		{
			name: "header and footer larger than the object",
			build: func(t *testing.T) []byte {
				t.Helper()
				seg := oneRecordSegment(t)
				binary.LittleEndian.PutUint32(seg[frameHeaderLenOffset:], math.MaxUint32)

				return seg
			},
			wantErr: ErrFrameTruncated,
		},
		{
			name: "body runs into the footer",
			build: func(t *testing.T) []byte {
				t.Helper()
				seg := oneRecordSegment(t)
				binary.LittleEndian.PutUint64(seg[frameBodyLenOffset:], uint64(len(record)+rblockSize))

				return seg
			},
			wantErr: ErrFrameTruncated,
		},
		{
			name: "footer cut short",
			build: func(t *testing.T) []byte {
				t.Helper()
				seg := oneRecordSegment(t)

				return seg[:len(seg)-1]
			},
			wantErr: ErrFrameTruncated,
		},
		{
			name: "empty body",
			build: func(t *testing.T) []byte {
				t.Helper()
				return buildSegment(t)
			},
			wantErr: ErrBadBodyLength,
		},
		{
			name: "body larger than the server restores",
			build: func(t *testing.T) []byte {
				t.Helper()
				return buildSegment(t, make([]byte, maxBodySize+rblockSize))
			},
			wantErr: ErrBadBodyLength,
		},
		{
			name: "body length not rblock aligned",
			build: func(t *testing.T) []byte {
				t.Helper()
				seg := oneRecordSegment(t)
				binary.LittleEndian.PutUint64(seg[frameBodyLenOffset:], uint64(len(record)-1))

				return seg
			},
			wantErr: ErrBadBodyLength,
		},
		{
			name: "body offset not rblock aligned",
			build: func(t *testing.T) []byte {
				t.Helper()
				return buildFrame(t, frameSpec{
					body:        record,
					recordCount: 1,
					headerLen:   misalignedHeader,
					footerLen:   frameFooterSize,
				})
			},
			wantErr: ErrBadBodyOffset,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseFrame(tt.build(t))
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
