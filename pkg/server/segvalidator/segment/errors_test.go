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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordError(t *testing.T) {
	t.Parallel()

	err := newRecordError(3, 192, ErrBadEndMark)

	want := "record 3 at offset 192: " + ErrBadEndMark.Error()
	require.EqualError(t, err, want)
	require.ErrorIs(t, err, ErrBadEndMark)
	require.ErrorIs(t, errors.Unwrap(err), ErrBadEndMark)

	var recErr *RecordError
	require.ErrorAs(t, err, &recErr)

	assert.Equal(t, 3, recErr.Index)
	assert.Equal(t, 192, recErr.Offset)
}
