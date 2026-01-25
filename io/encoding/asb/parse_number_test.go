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

package asb

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Original functions (kept for benchmark comparison)
// ============================================================================

func _readIntegerOriginal(src *countingReader, delim byte) (int64, error) {
	data, err := _readUntil(src, delim, false)
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(data, 10, 64)
}

func _readSizeOriginal(src *countingReader, delim byte) (uint32, error) {
	data, err := _readUntil(src, delim, false)
	if err != nil {
		return 0, err
	}

	num, err := strconv.ParseUint(data, 10, 32)

	return uint32(num), err
}

// ============================================================================
// Unit Tests for _readIntegerDirect
// ============================================================================

func TestReadSignedInt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		delim   byte
		want    int64
		wantErr bool
	}{
		// Positive cases
		{
			name:    "positive zero",
			input:   "0\n",
			delim:   '\n',
			want:    0,
			wantErr: false,
		},
		{
			name:    "positive small int",
			input:   "123\n",
			delim:   '\n',
			want:    123,
			wantErr: false,
		},
		{
			name:    "positive large int",
			input:   "1234567890\n",
			delim:   '\n',
			want:    1234567890,
			wantErr: false,
		},
		{
			name:    "positive max int64",
			input:   "9223372036854775807\n",
			delim:   '\n',
			want:    math.MaxInt64,
			wantErr: false,
		},
		{
			name:    "negative small int",
			input:   "-123\n",
			delim:   '\n',
			want:    -123,
			wantErr: false,
		},
		{
			name:    "negative large int",
			input:   "-1234567890\n",
			delim:   '\n',
			want:    -1234567890,
			wantErr: false,
		},
		{
			name:    "negative min int64",
			input:   "-9223372036854775808\n",
			delim:   '\n',
			want:    math.MinInt64,
			wantErr: false,
		},
		{
			name:    "different delimiter",
			input:   "42 rest",
			delim:   ' ',
			want:    42,
			wantErr: false,
		},
		// Negative cases
		{
			name:    "empty input",
			input:   "\n",
			delim:   '\n',
			want:    0,
			wantErr: true,
		},
		{
			name:    "invalid character in middle",
			input:   "12a3\n",
			delim:   '\n',
			want:    0,
			wantErr: true,
		},
		{
			name:    "invalid character at start",
			input:   "abc\n",
			delim:   '\n',
			want:    0,
			wantErr: true,
		},
		{
			name:    "double negative",
			input:   "--123\n",
			delim:   '\n',
			want:    0,
			wantErr: true,
		},
		{
			name:    "negative in middle",
			input:   "12-3\n",
			delim:   '\n',
			want:    0,
			wantErr: true,
		},
		{
			name:    "missing delimiter (EOF)",
			input:   "123",
			delim:   '\n',
			want:    0,
			wantErr: true,
		},
		{
			name:    "only negative sign",
			input:   "-\n",
			delim:   '\n',
			want:    0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			reader := newCountingReader(strings.NewReader(tt.input), "test")
			got, err := readSignedInt(reader, tt.delim)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

// ============================================================================
// Unit Tests for _readSizeDirect
// ============================================================================

func TestReadUnsignedInt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		delim   byte
		want    uint32
		wantErr bool
	}{
		// Positive cases
		{
			name:    "zero",
			input:   "0\n",
			delim:   '\n',
			want:    0,
			wantErr: false,
		},
		{
			name:    "small size",
			input:   "123\n",
			delim:   '\n',
			want:    123,
			wantErr: false,
		},
		{
			name:    "large size",
			input:   "1234567890\n",
			delim:   '\n',
			want:    1234567890,
			wantErr: false,
		},
		{
			name:    "max uint32",
			input:   "4294967295\n",
			delim:   '\n',
			want:    math.MaxUint32,
			wantErr: false,
		},
		{
			name:    "different delimiter",
			input:   "42 rest",
			delim:   ' ',
			want:    42,
			wantErr: false,
		},
		// Negative cases
		{
			name:    "empty input",
			input:   "\n",
			delim:   '\n',
			want:    0,
			wantErr: true,
		},
		{
			name:    "negative number",
			input:   "-123\n",
			delim:   '\n',
			want:    0,
			wantErr: true,
		},
		{
			name:    "invalid character in middle",
			input:   "12a3\n",
			delim:   '\n',
			want:    0,
			wantErr: true,
		},
		{
			name:    "invalid character at start",
			input:   "abc\n",
			delim:   '\n',
			want:    0,
			wantErr: true,
		},
		{
			name:    "overflow uint32",
			input:   fmt.Sprintf("%d\n", uint64(math.MaxUint32)+1),
			delim:   '\n',
			want:    math.MaxUint32,
			wantErr: true,
		},
		{
			name:    "large overflow",
			input:   "99999999999999999999\n",
			delim:   '\n',
			want:    math.MaxUint32,
			wantErr: true,
		},
		{
			name:    "missing delimiter (EOF)",
			input:   "123",
			delim:   '\n',
			want:    0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			reader := newCountingReader(strings.NewReader(tt.input), "test")
			got, err := readUnsignedInt(reader, tt.delim)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

// ============================================================================
// Equivalence Tests - ensure new functions match original behavior
// ============================================================================

func TestIntegerParsingEquivalence(t *testing.T) {
	t.Parallel()

	testCases := []string{
		"0\n",
		"1\n",
		"123\n",
		"1234567890\n",
		"9223372036854775807\n",
		"-1\n",
		"-123\n",
		"-1234567890\n",
		"-9223372036854775808\n",
	}

	for _, input := range testCases {
		t.Run(input[:len(input)-1], func(t *testing.T) {
			t.Parallel()

			// Test original
			reader1 := newCountingReader(strings.NewReader(input), "test")
			orig, origErr := _readIntegerOriginal(reader1, '\n')

			// Test optimized
			reader2 := newCountingReader(strings.NewReader(input), "test")
			opt, optErr := readSignedInt(reader2, '\n')

			assert.Equal(t, origErr != nil, optErr != nil, "error mismatch: original=%v, optimized=%v", origErr, optErr)
			assert.Equal(t, orig, opt, "value mismatch")
		})
	}
}

func TestSizeParsingEquivalence(t *testing.T) {
	t.Parallel()

	testCases := []string{
		"0\n",
		"1\n",
		"123\n",
		"1234567890\n",
		"4294967295\n",
	}

	for _, input := range testCases {
		t.Run(input[:len(input)-1], func(t *testing.T) {
			t.Parallel()

			// Test original
			reader1 := newCountingReader(strings.NewReader(input), "test")
			orig, origErr := _readSizeOriginal(reader1, '\n')

			// Test optimized
			reader2 := newCountingReader(strings.NewReader(input), "test")
			opt, optErr := readUnsignedInt(reader2, '\n')

			assert.Equal(t, origErr != nil, optErr != nil, "error mismatch: original=%v, optimized=%v", origErr, optErr)
			assert.Equal(t, orig, opt, "value mismatch")
		})
	}
}

// ============================================================================
// Side-by-Side Benchmarks
// ============================================================================

func BenchmarkIntegerParsing_Original(b *testing.B) {
	inputs := []struct {
		name  string
		value string
	}{
		{"Small", "123\n"},
		{"Medium", "1234567890\n"},
		{"Large", "9223372036854775807\n"},
		{"Negative", "-9223372036854775808\n"},
	}

	for _, input := range inputs {
		b.Run(input.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				reader := newCountingReader(strings.NewReader(input.value), "test")
				_, err := _readIntegerOriginal(reader, '\n')
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkIntegerParsing_Optimized(b *testing.B) {
	inputs := []struct {
		name  string
		value string
	}{
		{"Small", "123\n"},
		{"Medium", "1234567890\n"},
		{"Large", "9223372036854775807\n"},
		{"Negative", "-9223372036854775808\n"},
	}

	for _, input := range inputs {
		b.Run(input.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				reader := newCountingReader(strings.NewReader(input.value), "test")
				_, err := readSignedInt(reader, '\n')
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSizeParsing_Original(b *testing.B) {
	inputs := []struct {
		name  string
		value string
	}{
		{"Small", "123\n"},
		{"Medium", "1234567\n"},
		{"Large", "4294967295\n"},
	}

	for _, input := range inputs {
		b.Run(input.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				reader := newCountingReader(strings.NewReader(input.value), "test")
				_, err := _readSizeOriginal(reader, '\n')
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSizeParsing_Optimized(b *testing.B) {
	inputs := []struct {
		name  string
		value string
	}{
		{"Small", "123\n"},
		{"Medium", "1234567\n"},
		{"Large", "4294967295\n"},
	}

	for _, input := range inputs {
		b.Run(input.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				reader := newCountingReader(strings.NewReader(input.value), "test")
				_, err := readUnsignedInt(reader, '\n')
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
