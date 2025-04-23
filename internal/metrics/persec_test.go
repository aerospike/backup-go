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

const metricRecordsPerSecond = "rps"

func TestNewRPSCollector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		loggerLevel   slog.Level
		expectEnabled bool
	}{
		{
			name:          "debug level logger enables metrics",
			loggerLevel:   slog.LevelDebug,
			expectEnabled: true,
		},
		{
			name:          "info level logger disables metrics",
			loggerLevel:   slog.LevelInfo,
			expectEnabled: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Create a logger with the specified level
			logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
				Level: tc.loggerLevel,
			}))

			// Create a new PerSecondCollector
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			collector := NewPerSecondCollector(ctx, logger, metricRecordsPerSecond, true)

			// Verify the collector was initialized correctly
			assert.NotNil(t, collector)
			assert.Equal(t, ctx, collector.ctx)
			assert.NotNil(t, collector.Increment)

			// Test if Increment is functional based on logger level
			initialCount := collector.counter.Load()
			collector.Increment()

			if tc.expectEnabled {
				assert.Equal(t, uint64(1), collector.counter.Load())
			} else {
				assert.Equal(t, initialCount, collector.counter.Load())
			}
		})
	}
}

func TestRPSCollector_GetLastResult(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		collector      *PerSecondCollector
		expectedResult float64
	}{
		{
			name:           "nil collector returns 0",
			collector:      nil,
			expectedResult: 0,
		},
		{
			name: "collector with lastResult returns correct value",
			collector: &PerSecondCollector{
				lastResult: 42.5,
			},
			expectedResult: 42.5,
		},
		{
			name: "collector with zero lastResult returns 0",
			collector: &PerSecondCollector{
				lastResult: 0,
			},
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

func TestRPSCollector_Report(t *testing.T) {
	// Create a logger with debug level
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a new PerSecondCollector
	collector := NewPerSecondCollector(ctx, logger, metricRecordsPerSecond, true)
	assert.NotNil(t, collector)

	// Simulate some requests
	for i := 0; i < 10; i++ {
		collector.Increment()
	}

	// Wait a bit for the report goroutine to run
	time.Sleep(1500 * time.Millisecond)

	// Check that lastResult was updated
	result := collector.GetLastResult()
	assert.Greater(t, result, float64(0), "Expected RecordsPerSecond to be greater than 0")

	// Cancel the context to stop the report goroutine
	cancel()

	// Wait for the goroutine to exit
	time.Sleep(100 * time.Millisecond)

	// Simulate more requests after cancellation
	prevResult := collector.GetLastResult()
	for i := 0; i < 10; i++ {
		collector.Increment()
	}

	// Wait a bit to ensure no more reports are generated
	time.Sleep(1500 * time.Millisecond)

	// Check that lastResult hasn't changed
	assert.Equal(t, prevResult, collector.GetLastResult(), "Expected RecordsPerSecond to remain unchanged after context cancellation")
}

func TestRPSCollector_Increment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		loggerLevel slog.Level
		numCalls    int
		expected    uint64
	}{
		{
			name:        "debug level logger counts increments",
			loggerLevel: slog.LevelDebug,
			numCalls:    5,
			expected:    5,
		},
		{
			name:        "info level logger doesn't count increments",
			loggerLevel: slog.LevelInfo,
			numCalls:    5,
			expected:    0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Create a logger with the specified level
			logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
				Level: tc.loggerLevel,
			}))

			// Create a new PerSecondCollector
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			collector := NewPerSecondCollector(ctx, logger, metricRecordsPerSecond, true)

			// Call Increment the specified number of times
			for i := 0; i < tc.numCalls; i++ {
				collector.Increment()
			}

			// Verify the count
			assert.Equal(t, tc.expected, collector.counter.Load())
		})
	}
}
