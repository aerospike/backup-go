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
	"slices"
	"testing"
)

func TestDiff(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		s1       []int
		s2       []int
		expected []int
	}{
		{
			name:     "empty slices",
			s1:       []int{},
			s2:       []int{},
			expected: []int{},
		},
		{
			name:     "s1 empty",
			s1:       []int{},
			s2:       []int{1, 2, 3},
			expected: []int{},
		},
		{
			name:     "s2 empty",
			s1:       []int{1, 2, 3},
			s2:       []int{},
			expected: []int{1, 2, 3},
		},
		{
			name:     "identical slices",
			s1:       []int{1, 2, 3},
			s2:       []int{1, 2, 3},
			expected: []int{},
		},
		{
			name:     "no overlap",
			s1:       []int{1, 2, 3},
			s2:       []int{4, 5, 6},
			expected: []int{1, 2, 3},
		},
		{
			name:     "partial overlap",
			s1:       []int{1, 2, 3, 4},
			s2:       []int{3, 4, 5, 6},
			expected: []int{1, 2},
		},
		{
			name:     "s1 is subset of s2",
			s1:       []int{2, 3},
			s2:       []int{1, 2, 3, 4},
			expected: []int{},
		},
		{
			name:     "s2 is subset of s1",
			s1:       []int{1, 2, 3, 4},
			s2:       []int{2, 3},
			expected: []int{1, 4},
		},
		{
			name:     "duplicates in s1",
			s1:       []int{1, 2, 2, 3, 3, 3},
			s2:       []int{3, 4, 5},
			expected: []int{1, 2, 2},
		},
		{
			name:     "duplicates in s2",
			s1:       []int{1, 2, 3},
			s2:       []int{3, 3, 3, 4, 5},
			expected: []int{1, 2},
		},
		{
			name:     "duplicates in both",
			s1:       []int{1, 1, 2, 2, 3, 3},
			s2:       []int{2, 2, 3, 3, 4, 4},
			expected: []int{1, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := Diff(tt.s1, tt.s2)
			if !slices.Equal(result, tt.expected) {
				t.Errorf("Diff() = %v, want %v", result, tt.expected)
			}
		})
	}
}
