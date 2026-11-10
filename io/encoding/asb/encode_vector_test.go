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
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
	"github.com/segmentio/asm/base64"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestVectorDims returns a float32 vector with dims distinct elements.
func newTestVectorDims(tb testing.TB, dims int) *a.Vector {
	tb.Helper()

	elements := make([]float32, dims)
	for i := range elements {
		elements[i] = float32(dims + i)
	}

	vector, err := a.NewVectorFloat32(elements)
	require.NoError(tb, err)

	return vector
}

// TestAppendVectorBinReusesBuffers encodes vectors of different sizes in a row,
// so pooled buffers are reused, grown and shrunk. Every line must match the
// encoding of the vector's own bytes.
func TestAppendVectorBinReusesBuffers(t *testing.T) {
	t.Parallel()

	const binName = "vec"

	// Grow, shrink, then grow again.
	dimensions := []int{64, 3, 1536, 8, 65536}

	tests := []struct {
		name    string
		compact bool
	}{
		{name: "base64", compact: false},
		{name: "compact", compact: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			encoder := NewEncoder(NewEncoderConfig("test", tt.compact, true, models.SIndexInfo{}))

			var number [32]byte

			for _, dims := range dimensions {
				vector := newTestVectorDims(t, dims)
				wire := vector.Bytes()

				want := fmt.Appendf(nil, "- V! %s %d %s\n", binName, len(wire), wire)
				if !tt.compact {
					encoded := base64.StdEncoding.EncodeToString(wire)
					want = fmt.Appendf(nil, "- V %s %d %s\n", binName, len(encoded), encoded)
				}

				got, err := encoder.appendVectorBin(nil, binName, vector, &number)
				require.NoError(t, err)
				assert.Equal(t, want, got, "dims=%d", dims)
			}
		})
	}
}
