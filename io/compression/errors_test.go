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

package compression

import (
	"errors"
	"fmt"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
)

func TestIsCorruptedError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "ErrReservedBlockType",
			err:      zstd.ErrReservedBlockType,
			expected: true,
		},
		{
			name:     "ErrCompressedSizeTooBig",
			err:      zstd.ErrCompressedSizeTooBig,
			expected: true,
		},
		{
			name:     "ErrWindowSizeExceeded",
			err:      zstd.ErrWindowSizeExceeded,
			expected: true,
		},
		{
			name:     "ErrWindowSizeTooSmall",
			err:      zstd.ErrWindowSizeTooSmall,
			expected: true,
		},
		{
			name:     "ErrMagicMismatch",
			err:      zstd.ErrMagicMismatch,
			expected: true,
		},
		{
			name:     "wrapped ErrReservedBlockType",
			err:      fmt.Errorf("failed to decompress: %w", zstd.ErrReservedBlockType),
			expected: true,
		},
		{
			name:     "wrapped ErrCompressedSizeTooBig",
			err:      fmt.Errorf("read error: %w", zstd.ErrCompressedSizeTooBig),
			expected: true,
		},
		{
			name:     "wrapped ErrWindowSizeExceeded",
			err:      fmt.Errorf("window validation: %w", zstd.ErrWindowSizeExceeded),
			expected: true,
		},
		{
			name:     "wrapped ErrWindowSizeTooSmall",
			err:      fmt.Errorf("window check failed: %w", zstd.ErrWindowSizeTooSmall),
			expected: true,
		},
		{
			name:     "wrapped ErrMagicMismatch",
			err:      fmt.Errorf("validation failed: %w", zstd.ErrMagicMismatch),
			expected: true,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "other error",
			err:      errors.New("some other error"),
			expected: false,
		},
		{
			name:     "io.EOF error",
			err:      fmt.Errorf("unexpected EOF"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsCorruptedError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
