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
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/errclass"
	"github.com/segmentio/asm/base64"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDecoderReadVectorReusesBuffers decodes several vector bins of different
// sizes with one decoder. Every vector must stay intact after the decoder
// reuses its buffers for the next bins.
func TestDecoderReadVectorReusesBuffers(t *testing.T) {
	t.Parallel()

	// Grow, shrink, then grow again to exercise both buffer reuse and growth.
	dimensions := []int{64, 3, 1536, 8}

	tests := []struct {
		name          string
		base64Encoded bool
	}{
		{name: "base64", base64Encoded: true},
		{name: "compact", base64Encoded: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			want := make(a.BinMap, len(dimensions))

			var (
				input  strings.Builder
				maxRaw int
			)

			for i, dims := range dimensions {
				elements := make([]float32, dims)
				for j := range elements {
					elements[j] = float32(i*dims + j)
				}

				vector, err := a.NewVectorFloat32(elements)
				require.NoError(t, err)

				name := "vector" + strconv.Itoa(i)
				want[name] = vector

				prefix, data := "- V! ", string(vector.Bytes())
				if tt.base64Encoded {
					prefix, data = "- V ", base64.StdEncoding.EncodeToString(vector.Bytes())
				}

				maxRaw = max(maxRaw, len(data))

				fmt.Fprintf(&input, "%s%s %d %s\n", prefix, name, len(data), data)
			}

			r := &Decoder{reader: newTestCountingReader(input.String())}

			got, err := r.readBins(uint16(len(dimensions)))
			require.NoError(t, err)
			require.Len(t, got, len(want))

			for name, wantVector := range want {
				gotVector, ok := got[name].(*a.Vector)
				require.True(t, ok, "bin %s is %T, want *a.Vector", name, got[name])
				assert.True(t, wantVector.(*a.Vector).Equal(gotVector), "bin %s differs", name)
			}

			// The raw buffer only grows, so it fits the largest bin.
			assert.Equal(t, maxRaw, cap(r.vectorRaw))
		})
	}
}

func TestDecoderReadVectorErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		give          string
		base64Encoded bool
		wantErr       error
	}{
		{
			name: "invalid base64", give: "4 ****\n", base64Encoded: true,
			wantErr: errclass.ErrCorruptData,
		},
		{
			name: "invalid vector header", give: "4 MTIz\n", base64Encoded: true,
			wantErr: errclass.ErrCorruptData,
		},
		{name: "truncated compact", give: "10 abc"},
		{name: "missing size", give: "abc\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := &Decoder{reader: newTestCountingReader(tt.give)}

			_, err := r.readVector(tt.base64Encoded)
			require.Error(t, err)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

// TestDecoderReadVectorRejectsOversize checks that a size above the vector
// limit is rejected as corrupt data before the reusable buffer grows.
func TestDecoderReadVectorRejectsOversize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		giveSize      int
		base64Encoded bool
	}{
		{name: "base64", giveSize: vectorMaxBase64Size + 1, base64Encoded: true},
		{name: "compact", giveSize: vectorMaxWireSize + 1, base64Encoded: false},
		{name: "max uint32", giveSize: math.MaxUint32, base64Encoded: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := &Decoder{reader: newTestCountingReader(strconv.Itoa(tt.giveSize) + " abc\n")}

			_, err := r.readVector(tt.base64Encoded)
			require.ErrorIs(t, err, errclass.ErrCorruptData)
			assert.Zero(t, cap(r.vectorRaw))
		})
	}
}
