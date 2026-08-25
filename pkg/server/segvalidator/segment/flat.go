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
	"bytes"
	"encoding/binary"
	"fmt"
)

// Layout of a flat record on device.
const (
	// flatMagic marks the beginning of every flat record. The server's dirty
	// variant (0x037af202) is deliberately not accepted: it is only written
	// with commit-to-device, never for backup segments.
	flatMagic uint32 = 0x037af201

	// rblockSize is the allocation granularity of a record.
	rblockSize = 16
	// flatRecordHdrSize is the size of the fixed part of the record header.
	// The on-disk layout is 35 bytes: magic(4) + flags(4) + digest(20) +
	// last_update_time(40 bits) + generation(16 bits) packed into 7 bytes.
	flatRecordHdrSize = 35
	// endMarkSize is the size of the trailing marker.
	endMarkSize = 4

	// digestOffset is where the 20 byte digest starts inside the header.
	digestOffset = 8
	// endMarkHashSize is the number of header bytes covered by the end marker.
	endMarkHashSize = 25
	// endMarkMask clears the top bit the server never sets.
	endMarkMask = 0x7fffffff

	// lutGenOffset is where the packed last_update_time/generation word starts.
	lutGenOffset = 28

	// minRecordSize is the header rounded up to the rblock size.
	minRecordSize = (flatRecordHdrSize + rblockSize - 1) &^ (rblockSize - 1)

	// maxNRBlocks is the largest value the n_rblocks field can hold.
	maxNRBlocks = 0x7ffff
	// MaxRecordSize is the largest record a header can describe: 8 MiB.
	MaxRecordSize = (maxNRBlocks + 1) * rblockSize
)

// Bit layout of the flags word at offset 4.
const (
	flagNRBlocksMask  = 0x7ffff
	flagHasVoidTime   = 1 << 19
	flagHasSet        = 1 << 20
	flagHasKey        = 1 << 21
	flagHasBins       = 1 << 22
	flagIsCompressed  = 1 << 23
	flagXDRWrite      = 1 << 24
	flagHasExtraFlags = 1 << 25
	flagTreeIDShift   = 26
	flagTreeIDMask    = 0x3f
)

// Bit layout of the last_update_time/generation word at offset 28.
const (
	lutMask         = 0xffffffffff
	generationShift = 40
	generationMask  = 0xffff
)

// Record content limits.
const (
	binNameMaxSize = 16
	setNameMaxSize = 64
	recordMaxBins  = 0x7fff

	voidTimeSize = 4
)

// Bit layout of the optional extra flags byte. Bits 0..2 carry the XDR
// tombstone flavors, which affect nothing this parser reads; bits 3 and 4
// announce the MRT fields that follow the byte itself.
const (
	extraFlagsSize        = 1
	extraFlagsHasMRTID    = 1 << 3
	extraFlagsHasMRTOrigV = 1 << 4
	// extraFlagsUnused are the three high bits the server has not assigned. A
	// record that sets any of them was written by a newer format than this
	// parser understands.
	extraFlagsUnused = 0xe0

	// mrtIDSize is the width of the MRT id, a plain uint64.
	mrtIDSize = 8
	// mrtOrigVSize is the width of as_record_version: a 40 bit
	// last_update_time and a 16 bit generation packed into 7 bytes.
	mrtOrigVSize = 7
)

// Per bin metadata flags.
const (
	binHasMeta      = 0x80
	binHasLUT       = 0x01
	binHasSrcID     = 0x02
	binUnknownFlags = 0x10 | 0x08 | 0x04
	binLUTSize      = 5
	binSrcIDSize    = 1
)

// Particle type identifiers.
const (
	particleTypeNull     = 0
	particleTypeInteger  = 1
	particleTypeFloat    = 2
	particleTypeString   = 3
	particleTypeBlob     = 4
	particleTypeJavaBlob = 7
	particleTypeCSharp   = 8
	particleTypePython   = 9
	particleTypeRuby     = 10
	particleTypePHP      = 11
	particleTypeErlang   = 12
	particleTypeVector   = 16
	particleTypeBool     = 17
	particleTypeHLL      = 18
	particleTypeMap      = 19
	particleTypeList     = 20
	particleTypeGeoJSON  = 23
)

// Fixed sizes of the self describing particles.
const (
	integerHdrSize = 2 // type + size
	floatSize      = 9 // type + uint64
	boolSize       = 2 // type + value
	blobHdrSize    = 5 // type + uint32 size
)

// flatHeader is the fixed part of a record header. The digest is intentionally
// not copied out: it is only needed to compute the end marker, which reads it
// straight from the backing buffer.
type flatHeader struct {
	lastUpdateTime uint64
	magic          uint32
	nRBlocks       uint32
	treeID         uint32
	generation     uint16
	hasVoidTime    bool
	hasSet         bool
	hasKey         bool
	hasBins        bool
	isCompressed   bool
	xdrWrite       bool
	hasExtraFlags  bool
}

// recordSize returns the on-device size the header claims for its record.
func (h flatHeader) recordSize() int {
	return int(h.nRBlocks+1) * rblockSize
}

// flatMeta is the variable part of a record that precedes the bins.
type flatMeta struct {
	setName []byte
	key     []byte
	mrtID   uint64
	// end is the offset right after the metadata.
	end        int
	voidTime   uint32
	nBins      uint32
	extraFlags byte
}

// parseFlatHeader decodes the fixed header at the start of data.
func parseFlatHeader(data []byte) (flatHeader, error) {
	if len(data) < flatRecordHdrSize {
		return flatHeader{}, ErrHeaderTooShort
	}

	flags := binary.LittleEndian.Uint32(data[4:8])
	lutGen := readLutGen(data)

	return flatHeader{
		lastUpdateTime: lutGen & lutMask,
		magic:          binary.LittleEndian.Uint32(data[0:4]),
		nRBlocks:       flags & flagNRBlocksMask,
		treeID:         (flags >> flagTreeIDShift) & flagTreeIDMask,
		generation:     uint16((lutGen >> generationShift) & generationMask),
		hasVoidTime:    flags&flagHasVoidTime != 0,
		hasSet:         flags&flagHasSet != 0,
		hasKey:         flags&flagHasKey != 0,
		hasBins:        flags&flagHasBins != 0,
		isCompressed:   flags&flagIsCompressed != 0,
		xdrWrite:       flags&flagXDRWrite != 0,
		hasExtraFlags:  flags&flagHasExtraFlags != 0,
	}, nil
}

// readLutGen decodes the 7 byte last_update_time/generation field.
func readLutGen(data []byte) uint64 {
	const lutGenSize = flatRecordHdrSize - lutGenOffset

	var lutGen uint64

	for i := range lutGenSize {
		lutGen |= uint64(data[lutGenOffset+i]) << (8 * i)
	}

	return lutGen
}

// writeLutGen encodes generation and last_update_time into the header.
func writeLutGen(record []byte, generation uint16, lastUpdateTime uint64) {
	lutGen := (lastUpdateTime & lutMask) | (uint64(generation) << generationShift)

	for i := range flatRecordHdrSize - lutGenOffset {
		record[lutGenOffset+i] = byte(lutGen >> (8 * i))
	}
}

// validateRecord walks one whole record: metadata, bins and end marker. hdr
// must have been parsed from data[off:].
//
// The bytes between the end marker and the record's rblock boundary are not
// inspected. The server does not zero them either, so their content carries no
// meaning.
func validateRecord(hdr flatHeader, data []byte, off, size int) error {
	recordEnd := off + size
	parseEnd := recordEnd - endMarkSize

	meta, err := unpackMeta(hdr, data, off+flatRecordHdrSize, parseEnd)
	if err != nil {
		return err
	}

	// nBins is zero unless the record has bins, and the walk must run even
	// then: it is what rejects a record padded out with whole spare rblocks.
	contentEnd, err := checkBins(data, meta.end, parseEnd, meta.nBins)
	if err != nil {
		return err
	}

	if contentEnd > parseEnd {
		return ErrContentOverflow
	}

	mark := binary.LittleEndian.Uint32(data[contentEnd:])

	want := makeEndMark(data[off:])
	if mark != want {
		return fmt.Errorf("%w: got 0x%08x want 0x%08x", ErrBadEndMark, mark, want)
	}

	return nil
}

// unpackMeta decodes the optional metadata fields between the header and the
// bins. Compressed records must be filtered out by the caller: the compression
// metadata that follows the bin count is not readable with this parser.
func unpackMeta(hdr flatHeader, data []byte, off, end int) (flatMeta, error) {
	var (
		meta flatMeta
		err  error
	)

	pos := off

	if hdr.generation == 0 {
		return meta, ErrZeroGeneration
	}

	if hdr.hasExtraFlags {
		if meta.extraFlags, pos, err = readExtraFlags(data, pos, end); err != nil {
			return meta, err
		}
	}

	// Both MRT fields hang off the extra flags, so they cannot appear unless
	// the byte above was present.
	if meta.extraFlags&extraFlagsHasMRTID != 0 {
		if pos+mrtIDSize > end {
			return meta, ErrIncompleteMRTID
		}

		meta.mrtID = binary.LittleEndian.Uint64(data[pos:])
		pos += mrtIDSize
	}

	if meta.extraFlags&extraFlagsHasMRTOrigV != 0 {
		if pos+mrtOrigVSize > end {
			return meta, ErrIncompleteMRTOrigV
		}

		pos += mrtOrigVSize
	}

	if hdr.hasVoidTime {
		if pos+voidTimeSize > end {
			return meta, ErrIncompleteVoidTime
		}

		meta.voidTime = binary.LittleEndian.Uint32(data[pos:])
		pos += voidTimeSize
	}

	if hdr.hasSet {
		if meta.setName, pos, err = readSetName(data, pos, end); err != nil {
			return meta, err
		}
	}

	if hdr.hasKey {
		if meta.key, pos, err = readKey(data, pos, end); err != nil {
			return meta, err
		}
	}

	if hdr.hasBins {
		if meta.nBins, pos, err = readBinCount(data, pos, end); err != nil {
			return meta, err
		}
	}

	if pos > end {
		return meta, ErrIncompleteMeta
	}

	meta.end = pos

	return meta, nil
}

// readExtraFlags reads the extra flags byte and rejects the bits the server has
// not defined yet.
func readExtraFlags(data []byte, off, end int) (flags byte, next int, err error) {
	if off+extraFlagsSize > end {
		return 0, off, ErrIncompleteExtraFlags
	}

	flags = data[off]

	if flags&extraFlagsUnused != 0 {
		return 0, off, fmt.Errorf("%w: 0x%02x", ErrUnsupportedExtraFields, flags)
	}

	return flags, off + extraFlagsSize, nil
}

// readSetName reads the length prefixed set name.
func readSetName(data []byte, off, end int) (name []byte, next int, err error) {
	if off >= end {
		return nil, off, ErrIncompleteSetName
	}

	size := int(data[off])
	off++

	if size == 0 || size >= setNameMaxSize {
		return nil, off, fmt.Errorf("%w: %d", ErrBadSetNameLength, size)
	}

	if off+size > end {
		return nil, off, ErrIncompleteSetName
	}

	return data[off : off+size], off + size, nil
}

// readKey reads the uintvar sized user key.
func readKey(data []byte, off, end int) (key []byte, next int, err error) {
	size, next, err := readUintvar(data, off, end)
	if err != nil {
		return nil, off, fmt.Errorf("bad key size: %w", err)
	}

	if size == 0 {
		return nil, off, ErrZeroKeySize
	}

	if next+int(size) > end {
		return nil, off, ErrIncompleteKey
	}

	return data[next : next+int(size)], next + int(size), nil
}

// readBinCount reads and range checks the uintvar bin count.
func readBinCount(data []byte, off, end int) (count uint32, next int, err error) {
	count, next, err = readUintvar(data, off, end)
	if err != nil {
		return 0, off, fmt.Errorf("bad n-bins: %w", err)
	}

	if count == 0 || count > recordMaxBins {
		return 0, off, fmt.Errorf("%w: %d", ErrBadBinCount, count)
	}

	return count, next, nil
}

// readUintvar decodes the variable length integer encoding used by the server.
//
// The encoding is base 128, most significant group first, with the top bit of
// every byte but the last set as a continuation marker. Note that this is the
// opposite byte order from LEB128: 0x81 0x00 is 128, not 1.
func readUintvar(data []byte, off, end int) (val uint32, next int, err error) {
	if off >= end {
		return 0, off, ErrTruncatedUintvar
	}

	first := data[off]
	if first&0x80 == 0 {
		return uint32(first), off + 1, nil
	}

	if first == 0x80 {
		return 0, off, ErrLeadingZeroUvar
	}

	pos := off

	for {
		val |= uint32(data[pos] & 0x7f)
		val <<= 7
		pos++

		if pos >= end {
			return 0, off, ErrTruncatedUintvar
		}

		if data[pos]&0x80 == 0 {
			return val | uint32(data[pos]), pos + 1, nil
		}

		// The next group would shift significant bits off the top.
		if val&0xfe000000 != 0 {
			return 0, off, ErrUintvarTooLong
		}
	}
}

// checkBins walks nBins packed bins and returns the offset right after them.
func checkBins(data []byte, off, end int, nBins uint32) (int, error) {
	pos := off

	for range nBins {
		if pos >= end {
			return 0, ErrIncompleteBin
		}

		nameLen := int(data[pos])
		pos++

		if nameLen >= binNameMaxSize {
			return 0, fmt.Errorf("%w: %d", ErrBadBinNameLength, nameLen)
		}

		if pos+nameLen > end {
			return 0, ErrIncompleteBinName
		}

		pos += nameLen

		next, err := skipBin(data, pos, end)
		if err != nil {
			return 0, err
		}

		pos = next
	}

	if pos > end {
		return 0, ErrIncompleteBin
	}

	// A whole unread rblock after the bins means the record claims more space
	// than its content needs.
	if pos+rblockSize <= end {
		return 0, ErrExtraRBlocks
	}

	return pos, nil
}

// skipBin skips the optional bin metadata and the particle that follows it.
func skipBin(data []byte, off, end int) (int, error) {
	pos := off
	if pos >= end {
		return 0, ErrIncompleteBin
	}

	flags := data[pos]
	if flags&binHasMeta != 0 {
		newPos, err := skipBinMeta(pos, end, flags)
		if err != nil {
			return 0, err
		}

		pos = newPos
	}

	return skipParticle(data, pos, end)
}

// skipBinMeta skips the flags byte and the optional fields it announces. The
// server writes them in this order: LUT, then source id.
func skipBinMeta(pos, end int, flags byte) (int, error) {
	pos++
	if pos >= end {
		return 0, ErrIncompleteBinMeta
	}

	flags &^= binHasMeta

	if flags&binUnknownFlags != 0 {
		return 0, fmt.Errorf("%w: 0x%02x", ErrUnknownBinFlags, flags)
	}

	if flags&binHasLUT != 0 {
		if pos+binLUTSize > end {
			return 0, ErrIncompleteBinLUT
		}

		pos += binLUTSize
	}

	if flags&binHasSrcID != 0 {
		if pos+binSrcIDSize > end {
			return 0, ErrIncompleteBinSrcID
		}

		pos += binSrcIDSize
	}

	return pos, nil
}

// skipParticle skips one self describing particle value.
//
// The size checks below follow the server's from_flat functions rather than its
// skip_flat functions: skipping does not validate integer widths or bool
// values, but any record that reaches this parser has to survive a real read.
func skipParticle(data []byte, off, end int) (int, error) {
	if off >= end {
		return 0, ErrIncompleteParticle
	}

	switch typ := data[off]; typ {
	case particleTypeNull:
		// A bin tombstone: the type byte is the whole particle.
		return off + 1, nil
	case particleTypeInteger:
		return skipInteger(data, off, end)
	case particleTypeFloat:
		return skipFixed(data, off, end, floatSize, ErrIncompleteFloat)
	case particleTypeBool:
		return skipBool(data, off, end)
	case particleTypeString, particleTypeBlob, particleTypeJavaBlob,
		particleTypeCSharp, particleTypePython, particleTypeRuby,
		particleTypePHP, particleTypeErlang, particleTypeVector,
		particleTypeHLL, particleTypeMap, particleTypeList, particleTypeGeoJSON:
		return skipBlob(data, off, end)
	default:
		return 0, fmt.Errorf("%w: %d", ErrUnknownParticleType, typ)
	}
}

// skipInteger skips type(1) + size(1) + data(size).
func skipInteger(data []byte, off, end int) (int, error) {
	if off+integerHdrSize > end {
		return 0, ErrIncompleteInteger
	}

	size := int(data[off+1])

	switch size {
	case 1, 2, 4, 8:
	default:
		return 0, fmt.Errorf("%w: %d", ErrBadIntegerSize, size)
	}

	next := off + integerHdrSize + size
	if next > end {
		return 0, ErrIncompleteInteger
	}

	return next, nil
}

// skipFixed skips a particle of a constant size.
func skipFixed(_ []byte, off, end, size int, incomplete error) (int, error) {
	next := off + size
	if next > end {
		return 0, incomplete
	}

	return next, nil
}

// skipBool skips type(1) + value(1) and rejects out of range values.
func skipBool(data []byte, off, end int) (int, error) {
	if off+boolSize > end {
		return 0, ErrIncompleteBool
	}

	if v := data[off+1]; v > 1 {
		return 0, fmt.Errorf("%w: %d", ErrBadBoolValue, v)
	}

	return off + boolSize, nil
}

// skipBlob skips type(1) + size(4) + data(size).
func skipBlob(data []byte, off, end int) (int, error) {
	if off+blobHdrSize > end {
		return 0, ErrIncompleteBlob
	}

	size := int(binary.LittleEndian.Uint32(data[off+1:]))

	next := off + blobHdrSize + size
	// next < off guards against an overflowing size on 32 bit platforms.
	if next < off || next > end {
		return 0, ErrIncompleteBlob
	}

	return next, nil
}

// isZero reports whether p contains only zero bytes. bytes.Count is assembly
// optimized, which matters because this runs over the padding of every record.
func isZero(p []byte) bool {
	return bytes.Count(p, zeroByte) == len(p)
}

// zeroByte is the needle for isZero. It is a package level variable so the
// slice is not rebuilt on every call.
var zeroByte = []byte{0}
