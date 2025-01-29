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

import "testing"

func TestListToMap(t *testing.T) {
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
