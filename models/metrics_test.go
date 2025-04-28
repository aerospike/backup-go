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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetrics_Add(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		base    *Metrics
		metrics []*Metrics
		want    *Metrics
	}{
		{
			name:    "nil base and empty metrics returns nil",
			base:    nil,
			metrics: []*Metrics{},
			want:    nil,
		},
		{
			name: "nil base with metrics returns sum of metrics",
			base: nil,
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
			},
			want: &Metrics{
				PipelineReadQueueSize:  15,
				PipelineWriteQueueSize: 35,
				RecordsPerSecond:       55,
				KilobytesPerSecond:     75,
			},
		},
		{
			name: "base with nil metrics returns base",
			base: &Metrics{
				PipelineReadQueueSize:  10,
				PipelineWriteQueueSize: 20,
				RecordsPerSecond:       30,
				KilobytesPerSecond:     40,
			},
			metrics: []*Metrics{nil, nil},
			want: &Metrics{
				PipelineReadQueueSize:  10,
				PipelineWriteQueueSize: 20,
				RecordsPerSecond:       30,
				KilobytesPerSecond:     40,
			},
		},
		{
			name: "base with metrics returns sum",
			base: &Metrics{
				PipelineReadQueueSize:  10,
				PipelineWriteQueueSize: 20,
				RecordsPerSecond:       30,
				KilobytesPerSecond:     40,
			},
			metrics: []*Metrics{
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
				nil, // Test handling of nil metrics
			},
			want: &Metrics{
				PipelineReadQueueSize:  17,
				PipelineWriteQueueSize: 38,
				RecordsPerSecond:       59,
				KilobytesPerSecond:     80,
			},
		},
		{
			name: "adding zero metrics returns copy of base",
			base: &Metrics{
				PipelineReadQueueSize:  10,
				PipelineWriteQueueSize: 20,
				RecordsPerSecond:       30,
				KilobytesPerSecond:     40,
			},
			metrics: []*Metrics{},
			want: &Metrics{
				PipelineReadQueueSize:  10,
				PipelineWriteQueueSize: 20,
				RecordsPerSecond:       30,
				KilobytesPerSecond:     40,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.base.Add(tt.metrics...)

			// For the nil case
			if tt.want == nil {
				assert.Nil(t, got)
				return
			}

			assert.Equal(t, tt.want.PipelineReadQueueSize, got.PipelineReadQueueSize)
			assert.Equal(t, tt.want.PipelineWriteQueueSize, got.PipelineWriteQueueSize)
			assert.Equal(t, tt.want.RecordsPerSecond, got.RecordsPerSecond)
			assert.Equal(t, tt.want.KilobytesPerSecond, got.KilobytesPerSecond)

			// Additional test to ensure a copy was made and not a reference
			if tt.base != nil {
				tt.base.PipelineReadQueueSize += 1000
				assert.NotEqual(t, tt.base.PipelineReadQueueSize, got.PipelineReadQueueSize)
			}
		})
	}
}
