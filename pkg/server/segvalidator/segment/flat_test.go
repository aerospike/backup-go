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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The parsers below are reached through Validate only for the fields the
// server writes today, so they are exercised here directly: a record carrying
// an MRT id or a bin tombstone is one this package has to keep reading whether
// or not a fixture happens to hold one.

func TestParseFlatHeader(t *testing.T) {
	t.Parallel()

	const (
		wantTreeID     = uint32(5)
		wantNRBlocks   = uint32(3)
		wantGeneration = uint16(7)
		wantLUT        = uint64(0x123456789a)
	)

	flags := wantNRBlocks | flagHasVoidTime | flagHasSet | flagHasKey | flagHasBins |
		flagIsCompressed | flagXDRWrite | flagHasExtraFlags | (wantTreeID << flagTreeIDShift)

	data := make([]byte, flatRecordHdrSize)
	binary.LittleEndian.PutUint32(data[0:4], flatMagic)
	binary.LittleEndian.PutUint32(data[4:8], flags)
	writeLutGen(data, wantGeneration, wantLUT)

	hdr, err := parseFlatHeader(data)
	require.NoError(t, err)

	want := flatHeader{
		lastUpdateTime: wantLUT,
		magic:          flatMagic,
		nRBlocks:       wantNRBlocks,
		treeID:         wantTreeID,
		generation:     wantGeneration,
		hasVoidTime:    true,
		hasSet:         true,
		hasKey:         true,
		hasBins:        true,
		isCompressed:   true,
		xdrWrite:       true,
		hasExtraFlags:  true,
	}

	require.Equal(t, want, hdr)
	assert.Equal(t, int(wantNRBlocks+1)*rblockSize, hdr.recordSize())
}

func TestParseFlatHeader_TooShort(t *testing.T) {
	t.Parallel()

	_, err := parseFlatHeader(make([]byte, flatRecordHdrSize-1))
	require.ErrorIs(t, err, ErrHeaderTooShort)
}

func TestReadExtraFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		data      []byte
		wantErr   error
		wantFlags byte
		wantNext  int
	}{
		{
			name:      "no extra fields",
			data:      []byte{0x00},
			wantFlags: 0x00,
			wantNext:  1,
		},
		{
			name:      "mrt id and original version",
			data:      []byte{extraFlagsHasMRTID | extraFlagsHasMRTOrigV},
			wantFlags: extraFlagsHasMRTID | extraFlagsHasMRTOrigV,
			wantNext:  1,
		},
		{
			// Bits 0..2 are XDR tombstone flavors this parser reads over.
			name:      "xdr tombstone flavors are accepted",
			data:      []byte{0x07},
			wantFlags: 0x07,
			wantNext:  1,
		},
		{
			name:    "byte is missing",
			data:    []byte{},
			wantErr: ErrIncompleteExtraFlags,
		},
		{
			name:    "unassigned bit",
			data:    []byte{extraFlagsUnused & 0x20},
			wantErr: ErrUnsupportedExtraFields,
		},
		{
			name:    "all unassigned bits",
			data:    []byte{extraFlagsUnused},
			wantErr: ErrUnsupportedExtraFields,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			flags, next, err := readExtraFlags(tt.data, 0, len(tt.data))

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			require.Equal(t, tt.wantFlags, flags)
			require.Equal(t, tt.wantNext, next)
		})
	}
}

func TestReadSetName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantName string
		data     []byte
		wantErr  error
		wantNext int
	}{
		{
			name:     "named set",
			data:     append([]byte{byte(len(testSetName))}, testSetName...),
			wantName: testSetName,
			wantNext: 1 + len(testSetName),
		},
		{
			name:     "longest set name",
			data:     append([]byte{setNameMaxSize - 1}, make([]byte, setNameMaxSize-1)...),
			wantName: string(make([]byte, setNameMaxSize-1)),
			wantNext: setNameMaxSize,
		},
		{
			name:    "length byte is missing",
			data:    []byte{},
			wantErr: ErrIncompleteSetName,
		},
		{
			name:    "zero length",
			data:    []byte{0x00, 'a'},
			wantErr: ErrBadSetNameLength,
		},
		{
			name:    "length at the maximum",
			data:    append([]byte{setNameMaxSize}, make([]byte, setNameMaxSize)...),
			wantErr: ErrBadSetNameLength,
		},
		{
			name:    "name is cut short",
			data:    []byte{0x04, 'd', 'e'},
			wantErr: ErrIncompleteSetName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			name, next, err := readSetName(tt.data, 0, len(tt.data))

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			require.Equal(t, tt.wantName, string(name))
			require.Equal(t, tt.wantNext, next)
		})
	}
}

func TestReadKey(t *testing.T) {
	t.Parallel()

	longKey := append(appendUintvar(nil, 200), make([]byte, 200)...)

	tests := []struct {
		name     string
		data     []byte
		wantErr  error
		wantKey  int
		wantNext int
	}{
		{
			name:     "single byte size",
			data:     []byte{0x03, 'a', 'b', 'c'},
			wantKey:  3,
			wantNext: 4,
		},
		{
			name:     "multi byte size",
			data:     longKey,
			wantKey:  200,
			wantNext: len(longKey),
		},
		{
			name:    "size is missing",
			data:    []byte{},
			wantErr: ErrTruncatedUintvar,
		},
		{
			name:    "unreadable size",
			data:    []byte{0x81},
			wantErr: ErrTruncatedUintvar,
		},
		{
			name:    "zero size",
			data:    []byte{0x00},
			wantErr: ErrZeroKeySize,
		},
		{
			name:    "key is cut short",
			data:    []byte{0x04, 'a', 'b'},
			wantErr: ErrIncompleteKey,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key, next, err := readKey(tt.data, 0, len(tt.data))

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			require.Len(t, key, tt.wantKey)
			require.Equal(t, tt.wantNext, next)
		})
	}
}

func TestReadBinCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		data      []byte
		wantErr   error
		wantCount uint32
		wantNext  int
	}{
		{
			name:      "one bin",
			data:      appendUintvar(nil, 1),
			wantCount: 1,
			wantNext:  1,
		},
		{
			name:      "most bins a record may hold",
			data:      appendUintvar(nil, recordMaxBins),
			wantCount: recordMaxBins,
			wantNext:  len(appendUintvar(nil, recordMaxBins)),
		},
		{
			name:    "unreadable count",
			data:    []byte{0x80},
			wantErr: ErrLeadingZeroUvar,
		},
		{
			name:    "zero bins",
			data:    appendUintvar(nil, 0),
			wantErr: ErrBadBinCount,
		},
		{
			name:    "more bins than a record may hold",
			data:    appendUintvar(nil, recordMaxBins+1),
			wantErr: ErrBadBinCount,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			count, next, err := readBinCount(tt.data, 0, len(tt.data))

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			require.Equal(t, tt.wantCount, count)
			require.Equal(t, tt.wantNext, next)
		})
	}
}

func TestReadUintvar_TooLong(t *testing.T) {
	t.Parallel()

	// Five continuation groups carry 35 bits, which no longer fit a uint32.
	data := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0x00}

	_, _, err := readUintvar(data, 0, len(data))
	require.ErrorIs(t, err, ErrUintvarTooLong)
}

func TestUnpackMeta(t *testing.T) {
	t.Parallel()

	const (
		voidTime = uint32(0x11223344)
		mrtID    = uint64(0x0102030405060708)
	)

	// A record carrying every optional field, in the order the server writes
	// them: extra flags, MRT id, MRT original version, void-time, set, key,
	// bin count.
	full := []byte{extraFlagsHasMRTID | extraFlagsHasMRTOrigV}
	full = binary.LittleEndian.AppendUint64(full, mrtID)
	full = append(full, make([]byte, mrtOrigVSize)...)
	full = binary.LittleEndian.AppendUint32(full, voidTime)
	full = append(full, byte(len(testSetName)))
	full = append(full, testSetName...)
	full = append(full, 0x02, 'k', 'v')
	full = appendUintvar(full, 3)

	fullHdr := flatHeader{
		generation:    testGeneration,
		hasExtraFlags: true,
		hasVoidTime:   true,
		hasSet:        true,
		hasKey:        true,
		hasBins:       true,
	}

	meta, err := unpackMeta(fullHdr, full, 0, len(full))
	require.NoError(t, err)

	assert.Equal(t, mrtID, meta.mrtID, "mrtID")
	assert.Equal(t, voidTime, meta.voidTime, "voidTime")
	assert.Equal(t, testSetName, string(meta.setName), "setName")
	assert.Equal(t, "kv", string(meta.key), "key")
	assert.Equal(t, uint32(3), meta.nBins, "nBins")
	assert.Equal(t, len(full), meta.end, "end")
}

func TestUnpackMeta_Errors(t *testing.T) {
	t.Parallel()

	withFlags := func(f byte) []byte { return []byte{f} }

	tests := []struct {
		name    string
		hdr     flatHeader
		data    []byte
		off     int
		end     int
		wantErr error
	}{
		{
			name:    "zero generation",
			hdr:     flatHeader{},
			data:    []byte{},
			wantErr: ErrZeroGeneration,
		},
		{
			name:    "extra flags byte is missing",
			hdr:     flatHeader{generation: testGeneration, hasExtraFlags: true},
			data:    []byte{},
			wantErr: ErrIncompleteExtraFlags,
		},
		{
			name:    "unsupported extra fields",
			hdr:     flatHeader{generation: testGeneration, hasExtraFlags: true},
			data:    withFlags(extraFlagsUnused),
			wantErr: ErrUnsupportedExtraFields,
		},
		{
			name:    "mrt id is cut short",
			hdr:     flatHeader{generation: testGeneration, hasExtraFlags: true},
			data:    append(withFlags(extraFlagsHasMRTID), make([]byte, mrtIDSize-1)...),
			wantErr: ErrIncompleteMRTID,
		},
		{
			name:    "mrt original version is cut short",
			hdr:     flatHeader{generation: testGeneration, hasExtraFlags: true},
			data:    append(withFlags(extraFlagsHasMRTOrigV), make([]byte, mrtOrigVSize-1)...),
			wantErr: ErrIncompleteMRTOrigV,
		},
		{
			name:    "void-time is cut short",
			hdr:     flatHeader{generation: testGeneration, hasVoidTime: true},
			data:    make([]byte, voidTimeSize-1),
			wantErr: ErrIncompleteVoidTime,
		},
		{
			name:    "set name is cut short",
			hdr:     flatHeader{generation: testGeneration, hasSet: true},
			data:    []byte{0x04, 'd'},
			wantErr: ErrIncompleteSetName,
		},
		{
			name:    "key is cut short",
			hdr:     flatHeader{generation: testGeneration, hasKey: true},
			data:    []byte{0x04, 'k'},
			wantErr: ErrIncompleteKey,
		},
		{
			name:    "bin count is unreadable",
			hdr:     flatHeader{generation: testGeneration, hasBins: true},
			data:    []byte{0x00},
			wantErr: ErrBadBinCount,
		},
		{
			// Nothing to read and nowhere to read it from: the metadata
			// starts past the end of the record.
			name:    "metadata starts past the record",
			hdr:     flatHeader{generation: testGeneration},
			data:    make([]byte, 8),
			off:     8,
			end:     4,
			wantErr: ErrIncompleteMeta,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			end := tt.end
			if end == 0 {
				end = len(tt.data)
			}

			_, err := unpackMeta(tt.hdr, tt.data, tt.off, end)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// bin builds one packed bin: a name, then the particle bytes given.
func bin(name string, particle ...byte) []byte {
	out := append([]byte{byte(len(name))}, name...)

	return append(out, particle...)
}

// nullParticle is a bin tombstone, whose type byte is the whole particle.
var nullParticle = []byte{particleTypeNull}

func TestCheckBins(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    []byte
		wantErr error
		nBins   uint32
		off     int
		end     int
		wantEnd int
	}{
		{
			name:    "one bin",
			data:    bin(testBinName, nullParticle...),
			nBins:   1,
			wantEnd: 3,
		},
		{
			name:    "two bins",
			data:    concat(bin("a", nullParticle...), bin("b", nullParticle...)),
			nBins:   2,
			wantEnd: 6,
		},
		{
			name:    "no bins",
			data:    []byte{},
			nBins:   0,
			wantEnd: 0,
		},
		{
			name:    "bin is missing",
			data:    []byte{},
			nBins:   1,
			wantErr: ErrIncompleteBin,
		},
		{
			name:    "bin name too long",
			data:    append([]byte{binNameMaxSize}, make([]byte, binNameMaxSize)...),
			nBins:   1,
			wantErr: ErrBadBinNameLength,
		},
		{
			name:    "bin name is cut short",
			data:    []byte{0x04, 'n', 'a'},
			nBins:   1,
			wantErr: ErrIncompleteBinName,
		},
		{
			name:    "bins start past the end",
			data:    make([]byte, 8),
			nBins:   0,
			off:     8,
			end:     4,
			wantErr: ErrIncompleteBin,
		},
		{
			// A whole rblock the bins do not reach is a record claiming more
			// space than its content needs.
			name:    "a spare rblock follows the bins",
			data:    append(bin(testBinName, nullParticle...), make([]byte, rblockSize)...),
			nBins:   1,
			wantErr: ErrExtraRBlocks,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			end := tt.end
			if end == 0 {
				end = len(tt.data)
			}

			got, err := checkBins(tt.data, tt.off, end, tt.nBins)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			require.Equal(t, tt.wantEnd, got)
		})
	}
}

func TestSkipBin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		data     []byte
		wantErr  error
		wantNext int
	}{
		{
			name:     "particle without metadata",
			data:     []byte{particleTypeNull},
			wantNext: 1,
		},
		{
			name: "metadata with a last update time",
			data: append([]byte{binHasMeta | binHasLUT},
				append(make([]byte, binLUTSize), particleTypeNull)...),
			wantNext: 7,
		},
		{
			name: "metadata with a last update time and a source id",
			data: append([]byte{binHasMeta | binHasLUT | binHasSrcID},
				append(make([]byte, binLUTSize+binSrcIDSize), particleTypeNull)...),
			wantNext: 8,
		},
		{
			name:    "nothing to read",
			data:    []byte{},
			wantErr: ErrIncompleteBin,
		},
		{
			name:    "metadata flags are the last byte",
			data:    []byte{binHasMeta},
			wantErr: ErrIncompleteBinMeta,
		},
		{
			name:    "unknown metadata flags",
			data:    []byte{binHasMeta | binUnknownFlags, 0x00, particleTypeNull},
			wantErr: ErrUnknownBinFlags,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			next, err := skipBin(tt.data, 0, len(tt.data))

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			require.Equal(t, tt.wantNext, next)
		})
	}
}

func TestSkipBinMeta(t *testing.T) {
	t.Parallel()

	// skipBinMeta reads no data of its own: it is told the flags and walks the
	// fields they announce by size alone.
	tests := []struct {
		name     string
		wantErr  error
		flags    byte
		end      int
		wantNext int
	}{
		{
			name:     "no optional fields",
			flags:    binHasMeta,
			end:      16,
			wantNext: 1,
		},
		{
			name:     "last update time",
			flags:    binHasMeta | binHasLUT,
			end:      16,
			wantNext: 1 + binLUTSize,
		},
		{
			name:     "source id",
			flags:    binHasMeta | binHasSrcID,
			end:      16,
			wantNext: 1 + binSrcIDSize,
		},
		{
			name:     "both optional fields",
			flags:    binHasMeta | binHasLUT | binHasSrcID,
			end:      16,
			wantNext: 1 + binLUTSize + binSrcIDSize,
		},
		{
			name:    "flags byte is the last one",
			flags:   binHasMeta,
			end:     1,
			wantErr: ErrIncompleteBinMeta,
		},
		{
			name:    "unknown flags",
			flags:   binHasMeta | 0x04,
			end:     16,
			wantErr: ErrUnknownBinFlags,
		},
		{
			name:    "last update time does not fit",
			flags:   binHasMeta | binHasLUT,
			end:     1 + binLUTSize - 1,
			wantErr: ErrIncompleteBinLUT,
		},
		{
			name:    "source id does not fit",
			flags:   binHasMeta | binHasLUT | binHasSrcID,
			end:     1 + binLUTSize,
			wantErr: ErrIncompleteBinSrcID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			next, err := skipBinMeta(0, tt.end, tt.flags)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			require.Equal(t, tt.wantNext, next)
		})
	}
}

func TestSkipParticle(t *testing.T) {
	t.Parallel()

	blobOf := func(typ byte, size int) []byte {
		out := append([]byte{typ}, binary.LittleEndian.AppendUint32(nil, uint32(size))...)

		return append(out, make([]byte, size)...)
	}

	tests := []struct {
		name     string
		data     []byte
		wantErr  error
		wantNext int
	}{
		{
			name:     "bin tombstone",
			data:     []byte{particleTypeNull},
			wantNext: 1,
		},
		{
			name:     "integer",
			data:     []byte{particleTypeInteger, 8, 1, 2, 3, 4, 5, 6, 7, 8},
			wantNext: 10,
		},
		{
			name:     "float",
			data:     append([]byte{particleTypeFloat}, make([]byte, floatSize-1)...),
			wantNext: floatSize,
		},
		{
			name:     "bool",
			data:     []byte{particleTypeBool, 1},
			wantNext: boolSize,
		},
		{
			name:     "string",
			data:     blobOf(particleTypeString, 5),
			wantNext: blobHdrSize + 5,
		},
		{
			name:     "blob",
			data:     blobOf(particleTypeBlob, 3),
			wantNext: blobHdrSize + 3,
		},
		{
			name:     "list",
			data:     blobOf(particleTypeList, 2),
			wantNext: blobHdrSize + 2,
		},
		{
			name:     "map",
			data:     blobOf(particleTypeMap, 2),
			wantNext: blobHdrSize + 2,
		},
		{
			name:     "geojson",
			data:     blobOf(particleTypeGeoJSON, 1),
			wantNext: blobHdrSize + 1,
		},
		{
			name:     "hll",
			data:     blobOf(particleTypeHLL, 1),
			wantNext: blobHdrSize + 1,
		},
		{
			name:     "vector",
			data:     blobOf(particleTypeVector, 4),
			wantNext: blobHdrSize + 4,
		},
		{
			name:     "language blob",
			data:     blobOf(particleTypeJavaBlob, 1),
			wantNext: blobHdrSize + 1,
		},
		{
			name:    "nothing to read",
			data:    []byte{},
			wantErr: ErrIncompleteParticle,
		},
		{
			name:    "unknown type",
			data:    []byte{0x7f},
			wantErr: ErrUnknownParticleType,
		},
		{
			name:    "integer header is cut short",
			data:    []byte{particleTypeInteger},
			wantErr: ErrIncompleteInteger,
		},
		{
			name:    "bad integer size",
			data:    []byte{particleTypeInteger, 3, 1, 2, 3},
			wantErr: ErrBadIntegerSize,
		},
		{
			name:    "integer is cut short",
			data:    []byte{particleTypeInteger, 8, 1, 2, 3},
			wantErr: ErrIncompleteInteger,
		},
		{
			name:    "float is cut short",
			data:    append([]byte{particleTypeFloat}, make([]byte, floatSize-2)...),
			wantErr: ErrIncompleteFloat,
		},
		{
			name:    "bool is cut short",
			data:    []byte{particleTypeBool},
			wantErr: ErrIncompleteBool,
		},
		{
			name:    "bad bool value",
			data:    []byte{particleTypeBool, 2},
			wantErr: ErrBadBoolValue,
		},
		{
			name:    "blob header is cut short",
			data:    []byte{particleTypeBlob, 0x01, 0x00},
			wantErr: ErrIncompleteBlob,
		},
		{
			name:    "blob is cut short",
			data:    blobOf(particleTypeBlob, 4)[:blobHdrSize+2],
			wantErr: ErrIncompleteBlob,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			next, err := skipParticle(tt.data, 0, len(tt.data))

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			require.Equal(t, tt.wantNext, next)
		})
	}
}

func TestSkipInteger_AcceptedSizes(t *testing.T) {
	t.Parallel()

	for _, size := range []int{1, 2, 4, 8} {
		data := append([]byte{particleTypeInteger, byte(size)}, make([]byte, size)...)

		next, err := skipInteger(data, 0, len(data))
		require.NoError(t, err, "skipInteger() of a %d byte integer", size)
		assert.Equal(t, integerHdrSize+size, next)
	}
}

func TestSkipBool_Values(t *testing.T) {
	t.Parallel()

	for _, v := range []byte{0, 1} {
		data := []byte{particleTypeBool, v}

		next, err := skipBool(data, 0, len(data))
		require.NoError(t, err, "skipBool() of %d", v)
		assert.Equal(t, boolSize, next)
	}
}
