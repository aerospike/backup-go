package asb

import (
	"testing"
)

// control characters
var asbEscapedChars = map[byte]struct{}{
	'\\': {},
	' ':  {},
	'\n': {},
}

func escapeASB_old(s string) []byte {
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

// --- Benchmarks ---

var (
	cleanStr = "ThisIsAStandardAerospikeKeyName"
	dirtyStr = "Key With Spaces\nAnd\\Backslashes"
	longStr  = "AVeryLongStringThatOnlyNeedsEscapingAtTheVeryEnd "
)

func BenchmarkEscapeOld_Clean(b *testing.B) {
	for i := 0; i < b.N; i++ {
		escapeASB_old(cleanStr)
	}
}

func BenchmarkEscapeNew_Clean(b *testing.B) {
	for i := 0; i < b.N; i++ {
		escapeASB(cleanStr)
	}
}

func BenchmarkEscapeOld_Dirty(b *testing.B) {
	for i := 0; i < b.N; i++ {
		escapeASB_old(dirtyStr)
	}
}

func BenchmarkEscapeNew_Dirty(b *testing.B) {
	for i := 0; i < b.N; i++ {
		escapeASB(dirtyStr)
	}
}

func BenchmarkEscapeOld_LongTrailing(b *testing.B) {
	for i := 0; i < b.N; i++ {
		escapeASB_old(longStr)
	}
}

func BenchmarkEscapeNew_LongTrailing(b *testing.B) {
	for i := 0; i < b.N; i++ {
		escapeASB(longStr)
	}
}
