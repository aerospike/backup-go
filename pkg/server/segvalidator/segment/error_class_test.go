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
	"testing"

	"github.com/aerospike/backup-go/errclass"
	"github.com/stretchr/testify/require"
)

// TestErrorClasses checks that every segment sentinel carries a class, and
// that the two format-level ones are reported as unsupported rather than
// corrupt: an unknown field is a newer writer, not a damaged file.
func TestErrorClasses(t *testing.T) {
	t.Parallel()

	corrupt := []error{
		ErrEmptySegment,
		ErrNoRecords,
		ErrBadTailSlack,
		ErrHeaderTooShort,
		ErrBadMagic,
		ErrRecordTooSmall,
		ErrRecordOutOfBounds,
		ErrContentOverflow,
		ErrBadEndMark,
		ErrNonZeroPadding,
		ErrZeroGeneration,
		ErrBadSetNameLength,
		ErrZeroKeySize,
		ErrBadBinCount,
		ErrTruncatedUintvar,
		ErrLeadingZeroUvar,
		ErrUintvarTooLong,
		ErrUnknownBinFlags,
		ErrBadIntegerSize,
		ErrBadBoolValue,
	}

	unsupported := []error{
		ErrUnsupportedExtraFields,
		ErrUnknownParticleType,
	}

	tests := []struct {
		name  string
		errs  []error
		class error
	}{
		{name: "corrupt data", errs: corrupt, class: errclass.ErrCorruptData},
		{name: "unsupported", errs: unsupported, class: errclass.ErrUnsupported},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for _, err := range tt.errs {
				require.ErrorIs(t, err, tt.class, err.Error())
			}
		})
	}
}

// TestRecordError_KeepsClass makes sure the positional wrapper does not hide
// the class from callers.
func TestRecordError_KeepsClass(t *testing.T) {
	t.Parallel()

	err := newRecordError(3, 128, ErrBadMagic)

	require.ErrorIs(t, err, errclass.ErrCorruptData)
	require.ErrorIs(t, err, ErrBadMagic)
}
