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

package backup

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCalculateStats(t *testing.T) {
	tests := []struct {
		name     string
		data     []float64
		expected estimateStats
	}{
		{
			name: "Normal dataset",
			data: []float64{10, 20, 30, 40, 50},
			expected: estimateStats{
				Mean:     30.0,
				Variance: 250.0,
			},
		},
		{
			name: "Single value dataset",
			data: []float64{100},
			expected: estimateStats{
				Mean:     100.0,
				Variance: 0.0,
			},
		},
		{
			name:     "Empty dataset",
			data:     []float64{},
			expected: estimateStats{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := calculateStats(tt.data)
			assert.Equal(t, tt.expected.Mean, stats.Mean, "Mean should be calculated correctly")
			assert.Equal(t, tt.expected.Variance, stats.Variance, "Variance should be calculated correctly")
		})
	}
}

func TestConfidenceInterval(t *testing.T) {
	tests := []struct {
		name       string
		stats      estimateStats
		sampleSize int
		expectedLo float64
		expectedHi float64
	}{
		{
			name: "Normal dataset",
			stats: estimateStats{
				Mean:     42.0,
				Variance: 25.0,
			},
			sampleSize: 100,
			expectedLo: 40.712,
			expectedHi: 43.288,
		},
		{
			name: "Small sample size",
			stats: estimateStats{
				Mean:     50.0,
				Variance: 0.0,
			},
			sampleSize: 1,
			expectedLo: 50.0,
			expectedHi: 50.0,
		},
		{
			name: "Empty dataset",
			stats: estimateStats{
				Mean:     0.0,
				Variance: 0.0,
			},
			sampleSize: 0,
			expectedLo: 0.0,
			expectedHi: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			low, high := confidenceInterval(tt.stats, tt.sampleSize)
			assert.Equal(t, tt.expectedLo, low, "Low should be calculated correctly")
			assert.Equal(t, tt.expectedHi, high, "High should be calculated correctly")
		})
	}
}

func TestGetCompressRatio(t *testing.T) {
	policyCompressed := NewCompressionPolicy(CompressZSTD, 20)
	policyUnCompressed := NewCompressionPolicy(CompressNone, 0)
	sampleData := []byte("sample data for compression")

	tests := []struct {
		name   string
		policy *CompressionPolicy
		data   []byte
		result float64
	}{
		{
			name:   "Valid compression",
			policy: policyCompressed,
			data:   sampleData,
			result: 0.675,
		},
		{
			name:   "Uncompressed",
			policy: policyUnCompressed,
			data:   sampleData,
			result: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ratio, err := getCompressRatio(tt.policy, tt.data)
			assert.Nil(t, err, "Error should be nil")
			assert.Equal(t, tt.result, ratio, "Ratio should be correct")
		})
	}
}

func TestGetEstimate(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(nil, nil))

	tests := []struct {
		name     string
		data     []float64
		total    float64
		expected float64
	}{
		{
			name:     "Normal dataset",
			data:     []float64{10, 20, 30, 40, 50},
			total:    100,
			expected: 3000.0,
		},
		{
			name:     "Single value dataset",
			data:     []float64{100},
			total:    10,
			expected: 1000.0,
		},
		{
			name:     "Empty dataset",
			data:     []float64{},
			total:    100,
			expected: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getEstimate(tt.data, tt.total, logger)
			assert.Equal(t, tt.expected, result, "Estimate should be calculated correctly")
		})
	}
}
