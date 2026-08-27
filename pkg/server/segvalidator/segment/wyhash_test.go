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

package segment

import (
	"testing"
)

// newTestHeader builds a header filled with a deterministic digest.
func newTestHeader(seed byte) []byte {
	header := make([]byte, flatRecordHdrSize)
	for i := range testDigestSize {
		header[digestOffset+i] = seed + byte(i)
	}

	return header
}

// TestWyhash64_GoldenValues pins the hash to the values produced by
// cf_wyhash64 in the Aerospike server. They must never change: any drift here
// silently invalidates every end marker comparison.
func TestWyhash64_GoldenValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		length int
		want   uint64
	}{
		{name: "empty", length: 0, want: 0xda3421757d70ae2d},
		{name: "one byte", length: 1, want: 0x9c0aff191d7c3766},
		{name: "four bytes", length: 4, want: 0x038571adfbb334de},
		{name: "one block", length: 16, want: 0x3c7a010b466e670b},
		{name: "end mark width", length: 25, want: 0xa6cf3de1fb1b7f86},
		{name: "multi block", length: 100, want: 0xd43f79c77a71bc77},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key := make([]byte, tt.length)
			for i := range key {
				key[i] = byte(i + 1)
			}

			if got := wyhash64(key); got != tt.want {
				t.Fatalf("wyhash64() = %#016x, want %#016x", got, tt.want)
			}
		})
	}
}

func TestWyhash64_LengthClasses(t *testing.T) {
	t.Parallel()

	// One length per branch of the implementation: the short path, the 16 byte
	// block loop and the 48 byte interleaved loop.
	lengths := []int{0, 1, 3, 4, 8, 15, 16, 17, 25, 48, 49, 96, 200}

	seen := make(map[uint64]int, len(lengths))

	for _, length := range lengths {
		key := make([]byte, length)
		for i := range key {
			key[i] = byte(i + 1)
		}

		got := wyhash64(key)

		if again := wyhash64(key); again != got {
			t.Fatalf("wyhash64() is not stable for length %d", length)
		}

		if prev, ok := seen[got]; ok {
			t.Fatalf("wyhash64() collides for lengths %d and %d", prev, length)
		}

		seen[got] = length
	}
}

func TestMakeEndMark(t *testing.T) {
	t.Parallel()

	base := newTestHeader(1)
	mark := makeEndMark(base)

	t.Run("golden value", func(t *testing.T) {
		t.Parallel()

		const want = uint32(0x07ea27e1)

		if mark != want {
			t.Fatalf("makeEndMark() = %#08x, want %#08x", mark, want)
		}
	})

	t.Run("stable", func(t *testing.T) {
		t.Parallel()

		if got := makeEndMark(newTestHeader(1)); got != mark {
			t.Fatalf("makeEndMark() = %#x, want %#x", got, mark)
		}
	})

	t.Run("top bit is masked off", func(t *testing.T) {
		t.Parallel()

		for seed := range byte(64) {
			if got := makeEndMark(newTestHeader(seed)); got&^endMarkMask != 0 {
				t.Fatalf("makeEndMark() = %#x, top bit set", got)
			}
		}
	})

	t.Run("sensitive to every hashed byte", func(t *testing.T) {
		t.Parallel()

		for i := range endMarkHashSize {
			mutated := newTestHeader(1)
			mutated[digestOffset+i] ^= 0xff

			if got := makeEndMark(mutated); got == mark {
				t.Fatalf("makeEndMark() unchanged after flipping byte %d", i)
			}
		}
	})

	t.Run("ignores bytes outside the hashed range", func(t *testing.T) {
		t.Parallel()

		mutated := newTestHeader(1)
		for i := digestOffset + endMarkHashSize; i < flatRecordHdrSize; i++ {
			mutated[i] ^= 0xff
		}

		if got := makeEndMark(mutated); got != mark {
			t.Fatalf("makeEndMark() = %#x, want %#x", got, mark)
		}
	})
}
