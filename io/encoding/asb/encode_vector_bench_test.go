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
	"bytes"
	"strconv"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
)

// This file compares vector bin encoding on backup:
//   - copy: Vector.Bytes allocates a copy per bin (the previous implementation).
//   - append: Encoder.appendVectorBin, the current implementation. It uses
//     Vector.AppendBytes: straight into dst in compact mode, into a pooled
//     buffer in base64 mode.

type vectorEncodeFunc func(e *Encoder, dst []byte, v *a.Vector, number *[32]byte) ([]byte, error)

type vectorEncodeApproach struct {
	name   string
	encode vectorEncodeFunc
}

const benchVectorBinName = "vec"

var vectorEncodeApproaches = []vectorEncodeApproach{
	{
		name: "copy",
		encode: func(e *Encoder, dst []byte, v *a.Vector, number *[32]byte) ([]byte, error) {
			if e.config.Compact {
				return appendRawBin(dst, binVectorTypeCompactPrefix, benchVectorBinName, v.Bytes(), number), nil
			}

			return appendBase64Bin(dst, binVectorTypePrefix, benchVectorBinName, v.Bytes(), number), nil
		},
	},
	{
		name: "append",
		encode: func(e *Encoder, dst []byte, v *a.Vector, number *[32]byte) ([]byte, error) {
			return e.appendVectorBin(dst, benchVectorBinName, v, number)
		},
	},
}

// vectorBenchEncoders returns encoders for the base64 and compact modes.
func vectorBenchEncoders() map[string]*Encoder {
	return map[string]*Encoder{
		benchVectorBase64:  NewEncoder(NewEncoderConfig("test", false, models.SIndexInfo{})),
		benchVectorCompact: NewEncoder(NewEncoderConfig("test", true, models.SIndexInfo{})),
	}
}

// BenchmarkEncodeVector compares vector encode approaches on one goroutine.
// The output buffer is reused, like the per-token buffer in the writers.
//
// Run: go test -run '^$' -bench 'BenchmarkEncodeVector$' -count 10 ./io/encoding/asb/
func BenchmarkEncodeVector(b *testing.B) {
	dimensions := []int{128, 1536, 65536}
	encoders := vectorBenchEncoders()

	for _, encoding := range []string{benchVectorBase64, benchVectorCompact} {
		for _, dims := range dimensions {
			vector := newTestVectorDims(b, dims)

			for _, approach := range vectorEncodeApproaches {
				b.Run(encoding+"/dims="+strconv.Itoa(dims)+"/"+approach.name, func(b *testing.B) {
					runVectorEncode(b, encoders[encoding], vector, approach.encode)
				})
			}
		}
	}
}

func runVectorEncode(b *testing.B, e *Encoder, v *a.Vector, encode vectorEncodeFunc) {
	b.Helper()

	var number [32]byte

	// Sanity check: every approach must produce the same line.
	want, err := vectorEncodeApproaches[0].encode(e, nil, v, &number)
	if err != nil {
		b.Fatal(err)
	}

	dst, err := encode(e, nil, v, &number)
	if err != nil {
		b.Fatal(err)
	}

	if !bytes.Equal(want, dst) {
		b.Fatal("encoded vector line differs from the reference")
	}

	b.SetBytes(int64(len(v.Bytes())))
	b.ReportAllocs()

	for b.Loop() {
		dst, err = encode(e, dst[:0], v, &number)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEncodeVectorParallel runs the same approaches on all Ps with one
// shared Encoder, like parallel backup writers do.
//
// Run: go test -run '^$' -bench BenchmarkEncodeVectorParallel -count 10 ./io/encoding/asb/
func BenchmarkEncodeVectorParallel(b *testing.B) {
	dimensions := []int{1536, 65536}
	encoders := vectorBenchEncoders()

	for _, encoding := range []string{benchVectorBase64, benchVectorCompact} {
		for _, dims := range dimensions {
			vector := newTestVectorDims(b, dims)

			for _, approach := range vectorEncodeApproaches {
				b.Run(encoding+"/dims="+strconv.Itoa(dims)+"/"+approach.name, func(b *testing.B) {
					e := encoders[encoding]

					b.SetBytes(int64(len(vector.Bytes())))
					b.ReportAllocs()

					b.RunParallel(func(pb *testing.PB) {
						var (
							number [32]byte
							dst    []byte
							err    error
						)

						for pb.Next() {
							dst, err = approach.encode(e, dst[:0], vector, &number)
							if err != nil {
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
