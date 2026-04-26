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

package models

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetrics_SumMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		metrics []*Metrics
		want    *Metrics
	}{
		{
			name:    "empty metrics returns nil",
			metrics: []*Metrics{},
			want:    nil,
		},
		{
			name:    "only nil metrics returns empty metrics",
			metrics: []*Metrics{nil, nil, nil},
			want: &Metrics{
				PipelineReadQueueSize:  0,
				PipelineWriteQueueSize: 0,
				RecordsPerSecond:       0,
				KilobytesPerSecond:     0,
			},
		},
		{
			name: "single metrics returns copy",
			metrics: []*Metrics{
				{
					PipelineReadQueueSize:  10,
					PipelineWriteQueueSize: 20,
					RecordsPerSecond:       30,
					KilobytesPerSecond:     40,
				},
			},
			want: &Metrics{
				PipelineReadQueueSize:  10,
				PipelineWriteQueueSize: 20,
				RecordsPerSecond:       30,
				KilobytesPerSecond:     40,
			},
		},
		{
			name: "multiple metrics returns sum",
			metrics: []*Metrics{
				{
					PipelineReadQueueSize:  10,
					PipelineWriteQueueSize: 20,
					RecordsPerSecond:       30,
					KilobytesPerSecond:     40,
				},
				{
					PipelineReadQueueSize:  5,
					PipelineWriteQueueSize: 15,
					RecordsPerSecond:       25,
					KilobytesPerSecond:     35,
				},
				{
					PipelineReadQueueSize:  2,
					PipelineWriteQueueSize: 3,
					RecordsPerSecond:       4,
					KilobytesPerSecond:     5,
				},
			},
			want: &Metrics{
				PipelineReadQueueSize:  17,
				PipelineWriteQueueSize: 38,
				RecordsPerSecond:       59,
				KilobytesPerSecond:     80,
			},
		},
		{
			name: "mix of nil and non-nil metrics returns sum",
			metrics: []*Metrics{
				nil,
				{
					PipelineReadQueueSize:  5,
					PipelineWriteQueueSize: 15,
					RecordsPerSecond:       25,
					KilobytesPerSecond:     35,
				},
				nil,
				{
					PipelineReadQueueSize:  2,
					PipelineWriteQueueSize: 3,
					RecordsPerSecond:       4,
					KilobytesPerSecond:     5,
				},
				nil,
			},
			want: &Metrics{
				PipelineReadQueueSize:  7,
				PipelineWriteQueueSize: 18,
				RecordsPerSecond:       29,
				KilobytesPerSecond:     40,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := SumMetrics(tt.metrics...)

			if tt.want == nil {
				assert.Nil(t, got)
				return
			}

			assert.Equal(t, tt.want.PipelineReadQueueSize, got.PipelineReadQueueSize)
			assert.Equal(t, tt.want.PipelineWriteQueueSize, got.PipelineWriteQueueSize)
			assert.Equal(t, tt.want.RecordsPerSecond, got.RecordsPerSecond)
			assert.Equal(t, tt.want.KilobytesPerSecond, got.KilobytesPerSecond)

			for _, m := range tt.metrics {
				if m == nil {
					continue
				}

				originalValue := m.PipelineReadQueueSize
				m.PipelineReadQueueSize += 1000
				assert.NotEqual(t, m.PipelineReadQueueSize, got.PipelineReadQueueSize)
				m.PipelineReadQueueSize = originalValue
				break
			}
		})
	}
}

func TestMovingAverage(t *testing.T) {
	t.Parallel()

	t.Run("empty average returns zero", func(t *testing.T) {
		t.Parallel()
		ma := NewMovingAverage(5)
		require.Equal(t, uint64(0), ma.Average())
		require.Equal(t, 0, ma.HistorySize())
	})

	t.Run("single value", func(t *testing.T) {
		t.Parallel()
		ma := NewMovingAverage(5)
		ma.Add(100)
		require.Equal(t, uint64(100), ma.Average())
		require.Equal(t, 1, ma.HistorySize())
	})

	t.Run("multiple values within window", func(t *testing.T) {
		t.Parallel()
		ma := NewMovingAverage(5)
		ma.Add(10)
		ma.Add(20)
		ma.Add(30)
		require.Equal(t, uint64(20), ma.Average()) // (10+20+30)/3 = 20
		require.Equal(t, 3, ma.HistorySize())
	})

	t.Run("window overflow drops oldest values", func(t *testing.T) {
		t.Parallel()
		ma := NewMovingAverage(3)
		ma.Add(10)
		ma.Add(20)
		ma.Add(30)
		require.Equal(t, uint64(20), ma.Average()) // (10+20+30)/3 = 20
		require.Equal(t, 3, ma.HistorySize())

		ma.Add(40)
		require.Equal(t, uint64(30), ma.Average()) // (20+30+40)/3 = 30
		require.Equal(t, 3, ma.HistorySize())

		ma.Add(50)
		require.Equal(t, uint64(40), ma.Average()) // (30+40+50)/3 = 40
		require.Equal(t, 3, ma.HistorySize())
	})

	t.Run("window size of 1", func(t *testing.T) {
		t.Parallel()
		ma := NewMovingAverage(1)
		ma.Add(100)
		require.Equal(t, uint64(100), ma.Average())
		ma.Add(200)
		require.Equal(t, uint64(200), ma.Average())
		require.Equal(t, 1, ma.HistorySize())
	})
}

func TestMovingAverage_Concurrent(t *testing.T) {
	t.Parallel()

	ma := NewMovingAverage(100)
	var wg sync.WaitGroup
	numGoroutines := 10
	numOps := 1000

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				ma.Add(uint64(j))
				_ = ma.Average()
				_ = ma.HistorySize()
			}
		}()
	}

	wg.Wait()
	require.LessOrEqual(t, ma.HistorySize(), 100)
}
