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
	"fmt"
	"math"
)

// Layout of the ASBK frame the server wraps around every segment body:
//
//	header(32) | body: flat records | footer(16)
//
// The server writes multi-byte fields in host byte order, which is
// little-endian on every platform it supports. A segment written by a
// big-endian build reads back with version major 0x0100 and is refused as an
// unsupported version, exactly as the server refuses it. The regions are
// located through the length fields stored in the header, never through the
// fixed sizes, so a header or footer that grew in a later minor version is
// still skipped.
const (
	// frameMagic opens every segment object.
	frameMagic = "ASBK"
	// frameVersionMajor is the only frame version the server reads.
	frameVersionMajor = 1

	// framePrefixSize covers the magic and the version, the part of the
	// header every frame version shares.
	framePrefixSize = 8
	// frameHeaderSize and frameFooterSize are the minimum sizes of the v1
	// header and footer.
	frameHeaderSize = 32
	frameFooterSize = 16
	// frameOverhead is what the frame adds to the body of a written segment.
	frameOverhead = frameHeaderSize + frameFooterSize

	// frameBlockRecords is the only block type the server interprets: a body
	// of flat records.
	frameBlockRecords = 1

	// frameFlagsCriticalMask selects the flag bits a reader must understand.
	// None of them is known yet, so any set critical bit refuses the segment.
	// Unknown ancillary bits in the high byte are ignored.
	frameFlagsCriticalMask  = 0x00ff
	frameFlagsKnownCritical = 0x0000

	// maxBodySize is the largest body the server restores.
	maxBodySize = 8 << 20
	// MaxSegmentSize is the largest segment object the server fetches for a
	// restore: a full body in a v1 frame. The ceiling bounds the whole object,
	// so a segment whose frame grew past v1 has to shrink its body to fit; a
	// larger object is refused before its frame is read.
	MaxSegmentSize = maxBodySize + frameOverhead
)

// Field offsets inside the v1 header. The footer starts with the record count.
const (
	frameVersionMajorOffset = 4
	frameBlockTypeOffset    = 8
	frameFlagsOffset        = 10
	frameHeaderLenOffset    = 12
	frameBodyLenOffset      = 16
	frameFooterLenOffset    = 24
)

// frame is the decoded part of a segment frame the validator relies on.
type frame struct {
	// recordCount is the number of records the writer put into the body.
	recordCount uint64
	// bodyOff is where the body starts inside the segment object.
	bodyOff int
	// bodyLen is the size of the body in bytes.
	bodyLen int
}

// bodyEnd returns the offset right after the body.
func (f frame) bodyEnd() int {
	return f.bodyOff + f.bodyLen
}

// parseFrame decodes the frame of a segment object and locates its body. It
// applies the same checks, in the same order, as the server does before it
// restores a segment, so a segment it accepts is one the server walks.
func parseFrame(data []byte) (frame, error) {
	if len(data) < framePrefixSize {
		return frame{}, fmt.Errorf("%w: %d bytes", ErrFrameTruncated, len(data))
	}

	if string(data[:len(frameMagic)]) != frameMagic {
		return frame{}, fmt.Errorf("%w: 0x%08x", ErrBadFrameMagic, binary.LittleEndian.Uint32(data))
	}

	if v := binary.LittleEndian.Uint16(data[frameVersionMajorOffset:]); v != frameVersionMajor {
		return frame{}, fmt.Errorf("%w: %d", ErrUnsupportedFrameVersion, v)
	}

	if len(data) < frameHeaderSize {
		return frame{}, fmt.Errorf("%w: %d bytes", ErrFrameTruncated, len(data))
	}

	flags := binary.LittleEndian.Uint16(data[frameFlagsOffset:])
	if flags&frameFlagsCriticalMask&^frameFlagsKnownCritical != 0 {
		return frame{}, fmt.Errorf("%w: 0x%04x", ErrUnsupportedFrameFlags, flags)
	}

	if bt := binary.LittleEndian.Uint16(data[frameBlockTypeOffset:]); bt != frameBlockRecords {
		return frame{}, fmt.Errorf("%w: %d", ErrUnsupportedBlockType, bt)
	}

	headerLen := uint64(binary.LittleEndian.Uint32(data[frameHeaderLenOffset:]))
	if headerLen < frameHeaderSize {
		return frame{}, fmt.Errorf("%w: %d", ErrBadFrameHeaderLength, headerLen)
	}

	footerLen := uint64(binary.LittleEndian.Uint32(data[frameFooterLenOffset:]))
	if footerLen < frameFooterSize {
		return frame{}, fmt.Errorf("%w: %d", ErrBadFrameFooterLength, footerLen)
	}

	bodyLen := binary.LittleEndian.Uint64(data[frameBodyLenOffset:])
	if bodyLen > math.MaxUint32 {
		return frame{}, fmt.Errorf("%w: %d", ErrBadBodyLength, bodyLen)
	}

	// The first clause makes the subtraction in the second one safe. Summing
	// the three lengths instead could wrap.
	size := uint64(len(data))
	if headerLen+footerLen > size || bodyLen > size-headerLen-footerLen {
		return frame{}, fmt.Errorf("%w: header %d, body %d, footer %d, object %d",
			ErrFrameTruncated, headerLen, bodyLen, footerLen, size)
	}

	if bodyLen == 0 || bodyLen > maxBodySize || bodyLen%rblockSize != 0 {
		return frame{}, fmt.Errorf("%w: %d", ErrBadBodyLength, bodyLen)
	}

	if headerLen%rblockSize != 0 {
		return frame{}, fmt.Errorf("%w: %d", ErrBadBodyOffset, headerLen)
	}

	return frame{
		recordCount: binary.LittleEndian.Uint64(data[headerLen+bodyLen:]),
		bodyOff:     int(headerLen),
		bodyLen:     int(bodyLen),
	}, nil
}
