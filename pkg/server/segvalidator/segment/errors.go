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

	"github.com/aerospike/backup-go/models"
)

// Segment level errors.
var (
	// ErrEmptySegment is returned for a zero length payload.
	ErrEmptySegment = fmt.Errorf("%w: segment is empty", models.ErrCorruptData)
	// ErrNoRecords is returned when a payload carries no record at all.
	ErrNoRecords = fmt.Errorf("%w: segment contains no records", models.ErrCorruptData)
	// ErrBadTailSlack is returned when the bytes after the last record are not zero.
	ErrBadTailSlack = fmt.Errorf("%w: non-zero tail slack", models.ErrCorruptData)
)

// Record level errors.
var (
	ErrHeaderTooShort    = fmt.Errorf("%w: record header too short", models.ErrCorruptData)
	ErrBadMagic          = fmt.Errorf("%w: bad record magic", models.ErrCorruptData)
	ErrRecordTooSmall    = fmt.Errorf("%w: record size below minimum", models.ErrCorruptData)
	ErrRecordOutOfBounds = fmt.Errorf("%w: record extends past segment buffer", models.ErrCorruptData)
	ErrContentOverflow   = fmt.Errorf("%w: record content overflows record boundary", models.ErrCorruptData)
	ErrBadEndMark        = fmt.Errorf("%w: bad end marker", models.ErrCorruptData)
	ErrNonZeroPadding    = fmt.Errorf("%w: non-zero padding inside record", models.ErrCorruptData)
)

// Metadata level errors.
var (
	ErrZeroGeneration         = fmt.Errorf("%w: generation is zero", models.ErrCorruptData)
	ErrIncompleteExtraFlags   = fmt.Errorf("%w: incomplete extra flags", models.ErrCorruptData)
	ErrUnsupportedExtraFields = fmt.Errorf("%w: unsupported extra storage fields", models.ErrUnsupported)
	ErrIncompleteMRTID        = fmt.Errorf("%w: incomplete MRT id", models.ErrCorruptData)
	ErrIncompleteMRTOrigV     = fmt.Errorf("%w: incomplete MRT original version", models.ErrCorruptData)
	ErrIncompleteVoidTime     = fmt.Errorf("%w: incomplete void-time", models.ErrCorruptData)
	ErrIncompleteSetName      = fmt.Errorf("%w: incomplete set name", models.ErrCorruptData)
	ErrBadSetNameLength       = fmt.Errorf("%w: bad set name length", models.ErrCorruptData)
	ErrZeroKeySize            = fmt.Errorf("%w: key size is zero", models.ErrCorruptData)
	ErrIncompleteKey          = fmt.Errorf("%w: incomplete user key", models.ErrCorruptData)
	ErrBadBinCount            = fmt.Errorf("%w: bad n-bins", models.ErrCorruptData)
	ErrIncompleteMeta         = fmt.Errorf("%w: incomplete record metadata", models.ErrCorruptData)
)

// Uintvar errors.
var (
	ErrTruncatedUintvar = fmt.Errorf("%w: truncated uintvar", models.ErrCorruptData)
	ErrLeadingZeroUvar  = fmt.Errorf("%w: illegal leading zero in uintvar", models.ErrCorruptData)
	ErrUintvarTooLong   = fmt.Errorf("%w: uintvar too long", models.ErrCorruptData)
)

// Bin and particle level errors.
var (
	ErrIncompleteBin       = fmt.Errorf("%w: incomplete flat bin", models.ErrCorruptData)
	ErrIncompleteBinMeta   = fmt.Errorf("%w: incomplete flat bin metadata", models.ErrCorruptData)
	ErrBadBinNameLength    = fmt.Errorf("%w: bad flat bin name length", models.ErrCorruptData)
	ErrIncompleteBinName   = fmt.Errorf("%w: incomplete flat bin name", models.ErrCorruptData)
	ErrUnknownBinFlags     = fmt.Errorf("%w: unknown bin flags", models.ErrCorruptData)
	ErrIncompleteBinLUT    = fmt.Errorf("%w: incomplete flat bin LUT", models.ErrCorruptData)
	ErrIncompleteBinSrcID  = fmt.Errorf("%w: incomplete flat bin src-id", models.ErrCorruptData)
	ErrExtraRBlocks        = fmt.Errorf("%w: extra rblocks follow flat bins", models.ErrCorruptData)
	ErrIncompleteParticle  = fmt.Errorf("%w: incomplete flat particle", models.ErrCorruptData)
	ErrUnknownParticleType = fmt.Errorf("%w: unknown particle type", models.ErrUnsupported)
	ErrIncompleteInteger   = fmt.Errorf("%w: incomplete flat integer", models.ErrCorruptData)
	ErrBadIntegerSize      = fmt.Errorf("%w: bad flat integer size", models.ErrCorruptData)
	ErrIncompleteFloat     = fmt.Errorf("%w: incomplete flat float", models.ErrCorruptData)
	ErrIncompleteBool      = fmt.Errorf("%w: incomplete flat bool", models.ErrCorruptData)
	ErrBadBoolValue        = fmt.Errorf("%w: bad flat bool value", models.ErrCorruptData)
	ErrIncompleteBlob      = fmt.Errorf("%w: incomplete flat blob", models.ErrCorruptData)
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
