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
	"strings"
	"testing"
)

func TestCheckDuplicates_Ints(t *testing.T) {
	tests := []struct {
		name     string
		in       []int
		wantErr  bool
		wantPart string // substring expected in error message
	}{
		{"empty", nil, false, ""},
		{"single", []int{42}, false, ""},
		{"no-dupes", []int{1, 2, 3, 4, 5}, false, ""},
		{"one-dupe", []int{1, 2, 3, 2}, true, "[2]"},
		{"two-dupes", []int{1, 2, 3, 2, 5, 3}, true, "[2 3]"},
		{"many-of-same", []int{9, 9, 9, 9}, true, "[9]"},
		{"non-adjacent-dupes", []int{1, 2, 3, 4, 2, 5, 3}, true, "[2 3]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckDuplicates(tt.in)
			if tt.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantErr && tt.wantPart != "" && !strings.Contains(err.Error(), tt.wantPart) {
				t.Fatalf("error %q does not contain expected %q", err.Error(), tt.wantPart)
			}
		})
	}
}

func TestCheckDuplicates_Strings(t *testing.T) {
	tests := []struct {
		name     string
		in       []string
		wantErr  bool
		wantPart string
	}{
		{"empty", nil, false, ""},
		{"single", []string{"foo"}, false, ""},
		{"no-dupes", []string{"a", "b", "c"}, false, ""},
		{"one-dupe", []string{"a", "b", "a"}, true, "[a]"},
		{"two-dupes", []string{"foo", "bar", "foo", "baz", "bar", "foo"}, true, "[foo bar]"},
		{"case-sensitive", []string{"A", "a"}, false, ""},
		{"non-adjacent-dupes", []string{"x", "y", "z", "x", "q", "z"}, true, "[x z]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckDuplicates(tt.in)
			if tt.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantErr && tt.wantPart != "" && !strings.Contains(err.Error(), tt.wantPart) {
				t.Fatalf("error %q does not contain expected %q", err.Error(), tt.wantPart)
			}
		})
	}
}
