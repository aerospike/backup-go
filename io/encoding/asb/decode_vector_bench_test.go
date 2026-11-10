// Copyright 2024-2026 Aerospike, Inc.
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
	"math/rand/v2"
	"strconv"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/segmentio/asm/base64"
)

// This file compares vector bin decoding on restore:
//   - fresh: fresh slices from readBytesSized/readBase64BytesSized, then
//     a.NewVectorFromBytes copies them again (the previous implementation).
//   - scratch: Decoder.readVector, the current implementation. Bytes are read
//     (and base64-decoded) into reusable decoder buffers, then
//     a.NewVectorFromBytes makes the only owning copy.

const (
	benchVectorBase64  = "base64"
	benchVectorCompact = "compact"
)

// vectorSink keeps benchmark results alive so the compiler can't drop them.
var vectorSink *a.Vector

// cycleReader returns data over and over, so one countingReader serves the
// whole benchmark and its 1 MiB bufio buffer is not part of the measurement.
type cycleReader struct {
	data []byte
	off  int
}

func (r *cycleReader) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) {
		c := copy(p[n:], r.data[r.off:])
		n += c
		r.off = (r.off + c) % len(r.data)
	}

	return n, nil
}

// vectorBinPayload returns "<len> <data>\n", the part of a vector bin line
// that fetchBinValue reads, followed by the line end.
func vectorBinPayload(tb testing.TB, dims int, encoding string) (payload, wire []byte) {
	tb.Helper()

	rng := rand.New(rand.NewPCG(1, 2))
	elements := make([]float32, dims)

	for i := range elements {
		elements[i] = rng.Float32()
	}

	vec, err := a.NewVectorFloat32(elements)
	if err != nil {
		tb.Fatal(err)
	}

	wire = vec.Bytes()

	data := wire
	if encoding == benchVectorBase64 {
		data = []byte(base64.StdEncoding.EncodeToString(wire))
	}

	payload = strconv.AppendInt(nil, int64(len(data)), 10)
	payload = append(payload, ' ')
	payload = append(payload, data...)
	payload = append(payload, '\n')

	return payload, wire
}

type vectorDecodeFunc func(r *Decoder) (*a.Vector, error)

type vectorApproach struct {
	name   string
	decode vectorDecodeFunc
}

func vectorDecodeApproaches(encoding string) []vectorApproach {
	base64Encoded := encoding == benchVectorBase64

	readFresh := func(r *Decoder) ([]byte, error) {
		if base64Encoded {
			return readBase64BytesSized(r.reader, ' ')
		}

		return readBytesSized(r.reader, ' ')
	}

	return []vectorApproach{
		{
			name: "fresh",
			decode: func(r *Decoder) (*a.Vector, error) {
				val, err := readFresh(r)
				if err != nil {
					return nil, err
				}

				return decodeVector(val)
			},
		},
		{
			name: "scratch",
			decode: func(r *Decoder) (*a.Vector, error) {
				return r.readVector(base64Encoded)
			},
		},
	}
}

// newVectorBenchDecoder returns a decoder that reads payload in a loop.
func newVectorBenchDecoder(payload []byte) *Decoder {
	return &Decoder{reader: newCountingReader(&cycleReader{data: payload}, testFileName)}
}

// decodeVectorBin decodes one vector bin value and consumes the line end.
func decodeVectorBin(r *Decoder, decode vectorDecodeFunc) (*a.Vector, error) {
	v, err := decode(r)
	if err != nil {
		return nil, err
	}

	if err := expectChar(r.reader, asbNewLine); err != nil {
		return nil, err
	}

	return v, nil
}

func runVectorDecode(b *testing.B, payload, wire []byte, decode vectorDecodeFunc) {
	b.Helper()

	r := newVectorBenchDecoder(payload)

	// Sanity check: every approach must produce the same vector.
	got, err := decodeVectorBin(r, decode)
	if err != nil {
		b.Fatal(err)
	}

	want, err := a.NewVectorFromBytes(wire)
	if err != nil {
		b.Fatal(err)
	}

	if !got.Equal(want) {
		b.Fatal("decoded vector differs from the source vector")
	}

	b.SetBytes(int64(len(wire)))
	b.ReportAllocs()

	for b.Loop() {
		v, err := decodeVectorBin(r, decode)
		if err != nil {
			b.Fatal(err)
		}

		vectorSink = v
	}
}

// BenchmarkDecodeVector compares vector decode approaches on one goroutine.
//
// Run: go test -run '^$' -bench 'BenchmarkDecodeVector$' -count 10 ./io/encoding/asb/
func BenchmarkDecodeVector(b *testing.B) {
	// 128 and 1536 are common embedding sizes, 65536 float32 is the max (256 KiB).
	dimensions := []int{128, 1536, 65536}

	for _, encoding := range []string{benchVectorBase64, benchVectorCompact} {
		for _, dims := range dimensions {
			payload, wire := vectorBinPayload(b, dims, encoding)

			for _, approach := range vectorDecodeApproaches(encoding) {
				b.Run(encoding+"/dims="+strconv.Itoa(dims)+"/"+approach.name, func(b *testing.B) {
					runVectorDecode(b, payload, wire, approach.decode)
				})
			}
		}
	}
}

// BenchmarkDecodeVectorParallel runs the same approaches on all Ps, like
// parallel restore readers do, so GC pressure from allocations shows up in
// the time per op.
//
// Run: go test -run '^$' -bench BenchmarkDecodeVectorParallel -count 10 ./io/encoding/asb/
func BenchmarkDecodeVectorParallel(b *testing.B) {
	dimensions := []int{1536, 65536}

	for _, encoding := range []string{benchVectorBase64, benchVectorCompact} {
		for _, dims := range dimensions {
			payload, wire := vectorBinPayload(b, dims, encoding)

			for _, approach := range vectorDecodeApproaches(encoding) {
				b.Run(encoding+"/dims="+strconv.Itoa(dims)+"/"+approach.name, func(b *testing.B) {
					b.SetBytes(int64(len(wire)))
					b.ReportAllocs()

					b.RunParallel(func(pb *testing.PB) {
						r := newVectorBenchDecoder(payload)

						for pb.Next() {
							if _, err := decodeVectorBin(r, approach.decode); err != nil {
								b.Error(err)

								return
							}
						}
					})
				})
			}
		}
	}
}
