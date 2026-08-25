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
	"encoding/binary"
	"math/bits"
)

// Constants mirror cf_wyhash64 from
// modules/common/src/include/citrusleaf/cf_hash_math.h.
const (
	wySecret0 uint64 = 0xa0761d6478bd642f
	wySecret1 uint64 = 0xe7037ed1a0b428db
	wySecret2 uint64 = 0x8ebc6af09c88c6e3
	wySecret3 uint64 = 0x589965cc75374cc3

	wySeed uint64 = 0x29fbb14cc886f

	// wyBlockSize is the size of one mixing block of the long input path.
	wyBlockSize = 16
	// wyWideBlockSize is the size of the three-way interleaved block.
	wyWideBlockSize = 48
)

// makeEndMark computes the marker the server writes right after the record
// content. The C code hashes 25 bytes starting at keyd: the 20-byte digest plus
// the first 5 bytes of the following last_update_time/generation field.
//
// The caller must guarantee len(record) >= flatRecordHdrSize.
func makeEndMark(record []byte) uint32 {
	hash := wyhash64(record[digestOffset : digestOffset+endMarkHashSize])

	return uint32(hash) & endMarkMask
}

// wyhash64 hashes key with the seed baked into the Aerospike server.
func wyhash64(key []byte) uint64 {
	var (
		length = len(key)
		seed   = wySeed ^ wySecret0
		a, b   uint64
	)

	if length <= wyBlockSize {
		a, b = wyhashShort(key, length)
	} else {
		a, b, seed = wyhashLong(key, length, seed)
	}

	return wymix(wySecret1^uint64(length), wymix(a^wySecret1, b^seed))
}

// wyhashShort handles keys of at most 16 bytes.
func wyhashShort(key []byte, length int) (a, b uint64) {
	switch {
	case length >= 4:
		mid := (length >> 3) << 2
		a = wyr4(key)<<32 | wyr4(key[mid:])
		b = wyr4(key[length-4:])<<32 | wyr4(key[length-4-mid:])
	case length > 0:
		a = wyr3(key, length)
	}

	return a, b
}

// wyhashLong consumes keys longer than 16 bytes and returns the two trailing
// words together with the updated seed.
func wyhashLong(key []byte, length int, seed uint64) (a, b, out uint64) {
	var (
		off    int
		remain = length
	)

	if remain > wyWideBlockSize {
		see1, see2 := seed, seed

		for remain > wyWideBlockSize {
			seed = wymix(wyr8(key[off:])^wySecret1, wyr8(key[off+8:])^seed)
			see1 = wymix(wyr8(key[off+16:])^wySecret2, wyr8(key[off+24:])^see1)
			see2 = wymix(wyr8(key[off+32:])^wySecret3, wyr8(key[off+40:])^see2)
			off += wyWideBlockSize
			remain -= wyWideBlockSize
		}

		seed ^= see1 ^ see2
	}

	for remain > wyBlockSize {
		seed = wymix(wyr8(key[off:])^wySecret1, wyr8(key[off+8:])^seed)
		off += wyBlockSize
		remain -= wyBlockSize
	}

	a = wyr8(key[off+remain-16:])
	b = wyr8(key[off+remain-8:])

	return a, b, seed
}

// wymix folds a 128 bit product into a single word. bits.Mul64 compiles to a
// single MUL instruction, replacing the hand rolled 32x32 decomposition.
func wymix(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)

	return lo ^ hi
}

func wyr8(p []byte) uint64 {
	return binary.LittleEndian.Uint64(p)
}

func wyr4(p []byte) uint64 {
	return uint64(binary.LittleEndian.Uint32(p))
}

func wyr3(p []byte, k int) uint64 {
	return uint64(p[0])<<16 | uint64(p[k>>1])<<8 | uint64(p[k-1])
}
