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
	"testing"
)

func TestRecordError(t *testing.T) {
	t.Parallel()

	err := newRecordError(3, 192, ErrBadEndMark)

	const want = "record 3 at offset 192: bad end marker"
	if got := err.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}

	if !errors.Is(err, ErrBadEndMark) {
		t.Errorf("errors.Is(err, ErrBadEndMark) = false, want true")
	}

	if got := errors.Unwrap(err); !errors.Is(got, ErrBadEndMark) {
		t.Errorf("Unwrap() = %v, want %v", got, ErrBadEndMark)
	}

	var recErr *RecordError
	if !errors.As(error(err), &recErr) {
		t.Fatalf("errors.As(err, *RecordError) = false, want true")
	}

	if recErr.Index != 3 || recErr.Offset != 192 {
		t.Errorf("record located at index %d offset %d, want 3 and 192", recErr.Index, recErr.Offset)
	}
}
