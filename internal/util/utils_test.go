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

package util

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListToMap(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    []string
		expected map[string]bool
	}{
		{
			name:     "empty slice",
			input:    []string{},
			expected: map[string]bool{},
		},
		{
			name:     "single element",
			input:    []string{"one"},
			expected: map[string]bool{"one": true},
		},
		{
			name:     "multiple unique elements",
			input:    []string{"one", "two", "three"},
			expected: map[string]bool{"one": true, "two": true, "three": true},
		},
		{
			name:     "duplicate elements",
			input:    []string{"one", "one", "two"},
			expected: map[string]bool{"one": true, "two": true},
		},
		{
			name:     "empty string element",
			input:    []string{""},
			expected: map[string]bool{"": true},
		},
		{
			name:     "mixed elements",
			input:    []string{"", "one", "two", ""},
			expected: map[string]bool{"": true, "one": true, "two": true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ListToMap(tt.input)

			// Check if maps have the same length
			if len(result) != len(tt.expected) {
				t.Errorf("map length mismatch: got %d, want %d", len(result), len(tt.expected))
			}

			// Check if all expected keys exist with correct values
			for key := range tt.expected {
				if value, exists := result[key]; !exists || !value {
					t.Errorf("key %q: got %v, want true", key, value)
				}
			}

			// Check if there are no extra keys
			for key := range result {
				if _, exists := tt.expected[key]; !exists {
					t.Errorf("unexpected key in result: %q", key)
				}
			}
		})
	}
}

func TestGetFileNumber(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		filename      string
		expected      uint64
		expectedError error
	}{
		{
			name:          "valid filename format",
			filename:      "backup_2023_12345.asbx",
			expected:      12345,
			expectedError: nil,
		},
		{
			name:          "another valid filename format",
			filename:      "data_20230101_67890.asbx",
			expected:      67890,
			expectedError: nil,
		},
		{
			name:          "valid filename with large number",
			filename:      "file_prefix_18446744073709551615.asbx", // max uint64
			expected:      18446744073709551615,
			expectedError: nil,
		},
		{
			name:          "non asbx file",
			filename:      "backup_2023_12345.txt",
			expected:      0,
			expectedError: nil,
		},
		{
			name:          "empty filename",
			filename:      "",
			expected:      0,
			expectedError: nil,
		},
		{
			name:          "just extension",
			filename:      ".asbx",
			expected:      0,
			expectedError: fmt.Errorf("invalid file name %q", ".asbx"),
		},
		{
			name:          "insufficient parts",
			filename:      "backup_12345.asbx",
			expected:      0,
			expectedError: fmt.Errorf("invalid file name %q", "backup_12345.asbx"),
		},
		{
			name:     "non-numeric file number",
			filename: "backup_2023_abc.asbx",
			expected: 0,
			expectedError: fmt.Errorf("failed to parse file number %q: %w", "backup_2023_abc.asbx", &strconv.NumError{
				Func: "ParseUint",
				Num:  "abc",
				Err:  strconv.ErrSyntax,
			}),
		},
		{
			name:     "negative file number",
			filename: "backup_2023_-123.asbx",
			expected: 0,
			expectedError: fmt.Errorf("failed to parse file number %q: %w", "backup_2023_-123.asbx", &strconv.NumError{
				Func: "ParseUint",
				Num:  "-123",
				Err:  strconv.ErrSyntax,
			}),
		},
		{
			name:     "file number exceeds uint64",
			filename: "backup_2023_18446744073709551616.asbx", // uint64 max + 1
			expected: 0,
			expectedError: fmt.Errorf("failed to parse file number %q: %w", "backup_2023_18446744073709551616.asbx", &strconv.NumError{
				Func: "ParseUint",
				Num:  "18446744073709551616",
				Err:  strconv.ErrRange,
			}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result, err := GetFileNumber(tc.filename)

			if tc.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tc.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tc.expected, result)
		})
	}
}
