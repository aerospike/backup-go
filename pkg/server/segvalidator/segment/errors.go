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
	"fmt"

	"github.com/aerospike/backup-go/errclass"
)

// Segment level errors.
var (
	// ErrEmptySegment is returned for a zero length payload.
	ErrEmptySegment = fmt.Errorf("%w: segment is empty", errclass.ErrCorruptData)
	// ErrNoRecords is returned when a payload carries no record at all.
	ErrNoRecords = fmt.Errorf("%w: segment contains no records", errclass.ErrCorruptData)
	// ErrBadTailSlack is returned when the bytes after the last record are not zero.
	ErrBadTailSlack = fmt.Errorf("%w: non-zero tail slack", errclass.ErrCorruptData)
)

// Record level errors.
var (
	ErrHeaderTooShort    = fmt.Errorf("%w: record header too short", errclass.ErrCorruptData)
	ErrBadMagic          = fmt.Errorf("%w: bad record magic", errclass.ErrCorruptData)
	ErrRecordTooSmall    = fmt.Errorf("%w: record size below minimum", errclass.ErrCorruptData)
	ErrRecordOutOfBounds = fmt.Errorf("%w: record extends past segment buffer", errclass.ErrCorruptData)
	ErrContentOverflow   = fmt.Errorf("%w: record content overflows record boundary", errclass.ErrCorruptData)
	ErrBadEndMark        = fmt.Errorf("%w: bad end marker", errclass.ErrCorruptData)
	ErrNonZeroPadding    = fmt.Errorf("%w: non-zero padding inside record", errclass.ErrCorruptData)
)

// Metadata level errors.
var (
	ErrZeroGeneration         = fmt.Errorf("%w: generation is zero", errclass.ErrCorruptData)
	ErrIncompleteExtraFlags   = fmt.Errorf("%w: incomplete extra flags", errclass.ErrCorruptData)
	ErrUnsupportedExtraFields = fmt.Errorf("%w: unsupported extra storage fields", errclass.ErrUnsupported)
	ErrIncompleteMRTID        = fmt.Errorf("%w: incomplete MRT id", errclass.ErrCorruptData)
	ErrIncompleteMRTOrigV     = fmt.Errorf("%w: incomplete MRT original version", errclass.ErrCorruptData)
	ErrIncompleteVoidTime     = fmt.Errorf("%w: incomplete void-time", errclass.ErrCorruptData)
	ErrIncompleteSetName      = fmt.Errorf("%w: incomplete set name", errclass.ErrCorruptData)
	ErrBadSetNameLength       = fmt.Errorf("%w: bad set name length", errclass.ErrCorruptData)
	ErrZeroKeySize            = fmt.Errorf("%w: key size is zero", errclass.ErrCorruptData)
	ErrIncompleteKey          = fmt.Errorf("%w: incomplete user key", errclass.ErrCorruptData)
	ErrBadBinCount            = fmt.Errorf("%w: bad n-bins", errclass.ErrCorruptData)
	ErrIncompleteMeta         = fmt.Errorf("%w: incomplete record metadata", errclass.ErrCorruptData)
)

// Uintvar errors.
var (
	ErrTruncatedUintvar = fmt.Errorf("%w: truncated uintvar", errclass.ErrCorruptData)
	ErrLeadingZeroUvar  = fmt.Errorf("%w: illegal leading zero in uintvar", errclass.ErrCorruptData)
	ErrUintvarTooLong   = fmt.Errorf("%w: uintvar too long", errclass.ErrCorruptData)
)

// Bin and particle level errors.
var (
	ErrIncompleteBin       = fmt.Errorf("%w: incomplete flat bin", errclass.ErrCorruptData)
	ErrIncompleteBinMeta   = fmt.Errorf("%w: incomplete flat bin metadata", errclass.ErrCorruptData)
	ErrBadBinNameLength    = fmt.Errorf("%w: bad flat bin name length", errclass.ErrCorruptData)
	ErrIncompleteBinName   = fmt.Errorf("%w: incomplete flat bin name", errclass.ErrCorruptData)
	ErrUnknownBinFlags     = fmt.Errorf("%w: unknown bin flags", errclass.ErrCorruptData)
	ErrIncompleteBinLUT    = fmt.Errorf("%w: incomplete flat bin LUT", errclass.ErrCorruptData)
	ErrIncompleteBinSrcID  = fmt.Errorf("%w: incomplete flat bin src-id", errclass.ErrCorruptData)
	ErrExtraRBlocks        = fmt.Errorf("%w: extra rblocks follow flat bins", errclass.ErrCorruptData)
	ErrIncompleteParticle  = fmt.Errorf("%w: incomplete flat particle", errclass.ErrCorruptData)
	ErrUnknownParticleType = fmt.Errorf("%w: unknown particle type", errclass.ErrUnsupported)
	ErrIncompleteInteger   = fmt.Errorf("%w: incomplete flat integer", errclass.ErrCorruptData)
	ErrBadIntegerSize      = fmt.Errorf("%w: bad flat integer size", errclass.ErrCorruptData)
	ErrIncompleteFloat     = fmt.Errorf("%w: incomplete flat float", errclass.ErrCorruptData)
	ErrIncompleteBool      = fmt.Errorf("%w: incomplete flat bool", errclass.ErrCorruptData)
	ErrBadBoolValue        = fmt.Errorf("%w: bad flat bool value", errclass.ErrCorruptData)
	ErrIncompleteBlob      = fmt.Errorf("%w: incomplete flat blob", errclass.ErrCorruptData)
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
