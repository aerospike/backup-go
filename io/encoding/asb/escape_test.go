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
