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

package collections

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParallelMap_KeepsOrder(t *testing.T) {
	t.Parallel()

	in := []int{5, 1, 4, 2, 3}

	got, err := ParallelMap(t.Context(), 2, in, func(_ context.Context, v int) (string, error) {
		// Later elements finish first.
		time.Sleep(time.Duration(v) * time.Millisecond)
		return strconv.Itoa(v), nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"5", "1", "4", "2", "3"}, got)
}

func TestParallelMap_Empty(t *testing.T) {
	t.Parallel()

	got, err := ParallelMap(t.Context(), 2, nil, func(context.Context, int) (int, error) {
		t.Fatal("f called on no elements")
		return 0, nil
	})
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestParallelMap_Limit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		limit   int
		wantMax int64
	}{
		{name: "limited", limit: 3, wantMax: 3},
		{name: "unlimited", limit: 0, wantMax: 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var running, peak atomic.Int64

			release := make(chan struct{})
			in := make([]int, 20)

			go func() {
				// Wait until as many calls run as may, then let them all finish.
				for peak.Load() < tt.wantMax {
					time.Sleep(time.Millisecond)
				}

				close(release)
			}()

			_, err := ParallelMap(t.Context(), tt.limit, in, func(context.Context, int) (int, error) {
				n := running.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}

				<-release
				running.Add(-1)

				return 0, nil
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantMax, peak.Load())
		})
	}
}

func TestParallelMap_ErrorCancelsTheRest(t *testing.T) {
	t.Parallel()

	errFirst := errors.New("first")
	in := []int{0, 1, 2, 3}

	got, err := ParallelMap(t.Context(), 0, in, func(ctx context.Context, v int) (int, error) {
		if v == 0 {
			return 0, errFirst
		}

		// The others run until the failure cancels them.
		<-ctx.Done()

		return 0, ctx.Err()
	})
	require.ErrorIs(t, err, errFirst)
	assert.Nil(t, got)
}
