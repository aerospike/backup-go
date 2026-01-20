// Copyright 2026 Aerospike, Inc.
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

package asb

import (
	"bytes"
	"testing"
)

// control characters
var asbEscapedChars = map[byte]struct{}{
	'\\': {},
	' ':  {},
	'\n': {},
}

// deprecated function, keeping it to compare performance.
func escapeASBOld(s string) []byte {
	escapeCount := 0

	for _, c := range s {
		if _, ok := asbEscapedChars[byte(c)]; ok {
			escapeCount++
		}
	}

	if escapeCount == 0 {
		return []byte(s)
	}

	escaped := make([]byte, len(s)+escapeCount)
	i := 0

	for _, c := range s {
		if _, ok := asbEscapedChars[byte(c)]; ok {
			escaped[i] = '\\'
			i++
		}

		escaped[i] = byte(c)
		i++
	}

	return escaped
}

func TestEscapeASB(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []byte
	}{
		{
			name:     "Empty string",
			input:    "",
			expected: []byte(""),
		},
		{
			name:     "No special characters",
			input:    "Aerospike",
			expected: []byte("Aerospike"),
		},
		{
			name:     "Only spaces",
			input:    "  ",
			expected: []byte(`\ \ `),
		},
		{
			name:     "Start with special",
			input:    " name",
			expected: []byte(`\ name`),
		},
		{
			name:     "End with special",
			input:    "name\n",
			expected: []byte("name\\\n"),
		},
		{
			name:     "Backslash only",
			input:    "\\",
			expected: []byte(`\\`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test both to ensure logic parity
			resOld := escapeASBOld(tt.input)
			resNew := escapeASB(tt.input)

			if !bytes.Equal(resOld, tt.expected) {
				t.Errorf("Old implementation failed [%s]: expected %q, got %q", tt.name, tt.expected, resOld)
			}

			if !bytes.Equal(resNew, tt.expected) {
				t.Errorf("New implementation failed [%s]: expected %q, got %q", tt.name, tt.expected, resNew)
			}
		})
	}
}

// --- Benchmarks ---

var (
	cleanStr = "ThisIsAStandardAerospikeKeyName"
	dirtyStr = "Key With Spaces\nAnd\\Backslashes"
	longStr  = "AVeryLongStringThatOnlyNeedsEscapingAtTheVeryEnd "
)

func BenchmarkEscapeOld_Clean(b *testing.B) {
	for i := 0; i < b.N; i++ {
		escapeASBOld(cleanStr)
	}
}

func BenchmarkEscapeNew_Clean(b *testing.B) {
	for i := 0; i < b.N; i++ {
		escapeASB(cleanStr)
	}
}

func BenchmarkEscapeOld_Dirty(b *testing.B) {
	for i := 0; i < b.N; i++ {
		escapeASBOld(dirtyStr)
	}
}

func BenchmarkEscapeNew_Dirty(b *testing.B) {
	for i := 0; i < b.N; i++ {
		escapeASB(dirtyStr)
	}
}

func BenchmarkEscapeOld_LongTrailing(b *testing.B) {
	for i := 0; i < b.N; i++ {
		escapeASBOld(longStr)
	}
}

func BenchmarkEscapeNew_LongTrailing(b *testing.B) {
	for i := 0; i < b.N; i++ {
		escapeASB(longStr)
	}
}

/* Test results
goos: darwin
goarch: arm64
pkg: github.com/aerospike/backup-go/io/encoding/asb
cpu: Apple M1 Pro
BenchmarkEscapeOld_Clean
BenchmarkEscapeOld_Clean-10           	 5330002	       219.0 ns/op
BenchmarkEscapeNew_Clean
BenchmarkEscapeNew_Clean-10           	40598774	        29.72 ns/op
BenchmarkEscapeOld_Dirty
BenchmarkEscapeOld_Dirty-10           	 2212435	       540.5 ns/op
BenchmarkEscapeNew_Dirty
BenchmarkEscapeNew_Dirty-10           	18033327	        64.95 ns/op
BenchmarkEscapeOld_LongTrailing
BenchmarkEscapeOld_LongTrailing-10    	 1563889	       754.5 ns/op
BenchmarkEscapeNew_LongTrailing
BenchmarkEscapeNew_LongTrailing-10    	28877912	        41.75 ns/op
PASS

*/
