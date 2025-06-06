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

package metrics

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const testMetricMessage = "metric message"

func TestNewPerSecondCollector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		enabled       bool
		expectEnabled bool
	}{
		{
			name:          "enabled collector",
			enabled:       true,
			expectEnabled: true,
		},
		{
			name:          "disabled collector",
			enabled:       false,
			expectEnabled: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			}))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			collector := NewCollector(ctx, logger, RecordsPerSecond, testMetricMessage, tc.enabled)

			assert.NotNil(t, collector)
			assert.NotNil(t, collector.Increment)
			assert.Equal(t, tc.enabled, collector.enabled)

			initialCount := collector.processed.Load()
			collector.Increment()

			if tc.expectEnabled {
				assert.Equal(t, uint64(1), collector.processed.Load())
			} else {
				assert.Equal(t, initialCount, collector.processed.Load())
			}
		})
	}
}

func TestPerSecondCollector_GetLastResult(t *testing.T) {
	t.Parallel()

	collectorWithValue := &Collector{}
	collectorWithValue.lastResult.Store(42)

	collectorWithZero := &Collector{}
	collectorWithZero.lastResult.Store(0)

	tests := []struct {
		name           string
		collector      *Collector
		expectedResult uint64
	}{
		{
			name:           "nil collector returns 0",
			collector:      nil,
			expectedResult: 0,
		},
		{
			name:           "collector with lastResult returns correct value",
			collector:      collectorWithValue,
			expectedResult: 42,
		},
		{
			name:           "collector with zero lastResult returns 0",
			collector:      collectorWithZero,
			expectedResult: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := tc.collector.GetLastResult()
			assert.Equal(t, tc.expectedResult, result)
		})
	}
}

func TestPerSecondCollector_Report(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collector := NewCollector(ctx, logger, RecordsPerSecond, testMetricMessage, true)
	assert.NotNil(t, collector)

	for i := 0; i < 10; i++ {
		collector.Increment()
	}

	time.Sleep(1500 * time.Millisecond)

	result := collector.GetLastResult()
	assert.Greater(t, result, uint64(0), "Expected RecordsPerSecond to be greater than 0")

	cancel()

	time.Sleep(100 * time.Millisecond)

	prevResult := collector.GetLastResult()
	for i := 0; i < 10; i++ {
		collector.Increment()
	}

	time.Sleep(1500 * time.Millisecond)

	assert.Equal(t, prevResult, collector.GetLastResult(),
		"Expected RecordsPerSecond to remain unchanged after context cancellation")
}

func TestPerSecondCollector_Increment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		enabled  bool
		numCalls int
		expected uint64
	}{
		{
			name:     "enabled collector counts increments",
			enabled:  true,
			numCalls: 5,
			expected: 5,
		},
		{
			name:     "disabled collector doesn't count increments",
			enabled:  false,
			numCalls: 5,
			expected: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			}))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			collector := NewCollector(ctx, logger, RecordsPerSecond, testMetricMessage, tc.enabled)

			for i := 0; i < tc.numCalls; i++ {
				collector.Increment()
			}

			assert.Equal(t, tc.expected, collector.processed.Load())
		})
	}
}

func TestPerSecondCollector_Add(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		enabled  bool
		value    uint64
		expected uint64
	}{
		{
			name:     "enabled collector adds value",
			enabled:  true,
			value:    10,
			expected: 10,
		},
		{
			name:     "disabled collector doesn't add value",
			enabled:  false,
			value:    10,
			expected: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			}))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			collector := NewCollector(ctx, logger, RecordsPerSecond, testMetricMessage, tc.enabled)

			collector.Add(tc.value)

			assert.Equal(t, tc.expected, collector.processed.Load())
		})
	}
}

func TestPerSecondCollector_KilobytesPerSecond(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collector := NewCollector(ctx, logger, KilobytesPerSecond, testMetricMessage, true)
	assert.NotNil(t, collector)
	assert.Equal(t, KilobytesPerSecond.String(), collector.name)

	// because of rounding to uint, we should use 1024 for the test.
	collector.Add(1025)

	time.Sleep(1500 * time.Millisecond)

	result := collector.GetLastResult()
	assert.Greater(t, result, uint64(0), "Expected KilobytesPerSecond to be greater than 0")

	// The result should be approximately 1 KB/s (with some tolerance for timing variations)
	// Since we added 1024 bytes and waited ~1 second
	assert.InDelta(t, 1.0, result, 0.5, "Expected approximately 1 KB/s")

	cancel()
}
