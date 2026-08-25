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
	"errors"
	"fmt"
)

// Segment level errors.
var (
	// ErrEmptySegment is returned for a zero length payload.
	ErrEmptySegment = errors.New("segment is empty")
	// ErrNoRecords is returned when a payload carries no record at all.
	ErrNoRecords = errors.New("segment contains no records")
	// ErrBadTailSlack is returned when the bytes after the last record are not zero.
	ErrBadTailSlack = errors.New("non-zero tail slack")
)

// Record level errors.
var (
	ErrHeaderTooShort    = errors.New("record header too short")
	ErrBadMagic          = errors.New("bad record magic")
	ErrRecordTooSmall    = errors.New("record size below minimum")
	ErrRecordOutOfBounds = errors.New("record extends past segment buffer")
	ErrContentOverflow   = errors.New("record content overflows record boundary")
	ErrBadEndMark        = errors.New("bad end marker")
	ErrNonZeroPadding    = errors.New("non-zero padding inside record")
)

// Metadata level errors.
var (
	ErrZeroGeneration         = errors.New("generation is zero")
	ErrIncompleteExtraFlags   = errors.New("incomplete extra flags")
	ErrUnsupportedExtraFields = errors.New("unsupported extra storage fields")
	ErrIncompleteMRTID        = errors.New("incomplete MRT id")
	ErrIncompleteMRTOrigV     = errors.New("incomplete MRT original version")
	ErrIncompleteVoidTime     = errors.New("incomplete void-time")
	ErrIncompleteSetName      = errors.New("incomplete set name")
	ErrBadSetNameLength       = errors.New("bad set name length")
	ErrZeroKeySize            = errors.New("key size is zero")
	ErrIncompleteKey          = errors.New("incomplete user key")
	ErrBadBinCount            = errors.New("bad n-bins")
	ErrIncompleteMeta         = errors.New("incomplete record metadata")
)

// Uintvar errors.
var (
	ErrTruncatedUintvar = errors.New("truncated uintvar")
	ErrLeadingZeroUvar  = errors.New("illegal leading zero in uintvar")
	ErrUintvarTooLong   = errors.New("uintvar too long")
)

// Bin and particle level errors.
var (
	ErrIncompleteBin       = errors.New("incomplete flat bin")
	ErrIncompleteBinMeta   = errors.New("incomplete flat bin metadata")
	ErrBadBinNameLength    = errors.New("bad flat bin name length")
	ErrIncompleteBinName   = errors.New("incomplete flat bin name")
	ErrUnknownBinFlags     = errors.New("unknown bin flags")
	ErrIncompleteBinLUT    = errors.New("incomplete flat bin LUT")
	ErrIncompleteBinSrcID  = errors.New("incomplete flat bin src-id")
	ErrExtraRBlocks        = errors.New("extra rblocks follow flat bins")
	ErrIncompleteParticle  = errors.New("incomplete flat particle")
	ErrUnknownParticleType = errors.New("unknown particle type")
	ErrIncompleteInteger   = errors.New("incomplete flat integer")
	ErrBadIntegerSize      = errors.New("bad flat integer size")
	ErrIncompleteFloat     = errors.New("incomplete flat float")
	ErrIncompleteBool      = errors.New("incomplete flat bool")
	ErrBadBoolValue        = errors.New("bad flat bool value")
	ErrIncompleteBlob      = errors.New("incomplete flat blob")
)

// RecordError locates the record inside a segment that failed validation.
// It wraps the underlying cause, so errors.Is works against the sentinels above.
type RecordError struct {
	Err error
	// Index is the zero based position of the record within the segment.
	Index int
	// Offset is the byte offset of the record within the segment.
	Offset int
}

// Error implements the error interface.
func (e *RecordError) Error() string {
	return fmt.Sprintf("record %d at offset %d: %v", e.Index, e.Offset, e.Err)
}

// Unwrap returns the underlying cause.
func (e *RecordError) Unwrap() error {
	return e.Err
}

// newRecordError wraps err with the record position.
func newRecordError(index, offset int, err error) *RecordError {
	return &RecordError{Index: index, Offset: offset, Err: err}
}
