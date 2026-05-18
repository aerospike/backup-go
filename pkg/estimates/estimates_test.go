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

package estimates

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalculateEstimatedEndTime(t *testing.T) {
	tests := []struct {
		name        string
		startTime   time.Time
		percentDone float64
		expected    time.Duration
	}{
		{
			name:        "Zero Percent Done",
			startTime:   time.Now().Add(-10 * time.Second),
			percentDone: 0,
			expected:    time.Duration(0),
		},
		{
			name:        "Less Than 1 Percent Done",
			startTime:   time.Now().Add(-10 * time.Second),
			percentDone: 0.005,
			expected:    time.Duration(0),
		},
		{
			name:        "10 Percent Done",
			startTime:   time.Now().Add(-10 * time.Second),
			percentDone: 0.1,
			expected:    90 * time.Second,
		},
		{
			name:        "50 Percent Done",
			startTime:   time.Now().Add(-10 * time.Second),
			percentDone: 0.5,
			expected:    10 * time.Second,
		},
		{
			name:        "100 Percent Done",
			startTime:   time.Now().Add(-10 * time.Second),
			percentDone: 1.0,
			expected:    0 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateEstimatedEndTime(tt.startTime, tt.percentDone)

			if tt.percentDone < 0.01 {
				assert.Equal(t, tt.expected, result)
			} else {
				assert.InDelta(t, tt.expected.Milliseconds(), result.Milliseconds(), 100)
			}
		})
	}
}

func TestPrintBackupEstimate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	stats := models.NewBackupStats()
	stats.Start()
	stats.TotalRecords.Add(100)
	stats.ReadRecords.Add(50)

	logger := slog.New(slog.DiscardHandler)

	gm := func() *models.Metrics {
		return &models.Metrics{
			KilobytesPerSecond: 1000,
			RecordsPerSecond:   1000,
		}
	}

	done := make(chan struct{})

	go func() {
		PrintBackupEstimate(ctx, stats, gm, logger)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("PrintBackupEstimate did not complete in time")
	}
}

func TestPrintRestoreEstimate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	stats := models.NewRestoreStats()
	stats.Start()
	stats.IncrRecordsInserted()
	stats.RecordsSkipped.Add(1)
	stats.IncrRecordsExisted()

	logger := slog.New(slog.DiscardHandler)

	gm := func() *models.Metrics {
		return &models.Metrics{
			KilobytesPerSecond: 1000,
			RecordsPerSecond:   1000,
		}
	}

	gs := func() int64 {
		return 10000
	}

	done := make(chan struct{})

	go func() {
		PrintRestoreEstimate(ctx, stats, gm, gs, logger)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("PrintRestoreEstimate did not complete in time")
	}
}

func TestPrintEstimate(t *testing.T) {
	tests := []struct {
		name        string
		startTime   time.Time
		done        float64
		total       float64
		previousVal float64
		getMetrics  func() *models.Metrics
		wantErr     error
		expectLog   bool
	}{
		{
			name:        "completion at 100%",
			startTime:   time.Now().Add(-10 * time.Second),
			done:        100,
			total:       100,
			previousVal: 0.5,
			getMetrics: func() *models.Metrics {
				return &models.Metrics{
					RecordsPerSecond:   1000,
					KilobytesPerSecond: 1000,
				}
			},
			wantErr:   errBrake,
			expectLog: false,
		},
		{
			name:        "less than 1% completed",
			startTime:   time.Now().Add(-10 * time.Second),
			done:        0.5,
			total:       100,
			previousVal: 0,
			getMetrics: func() *models.Metrics {
				return &models.Metrics{}
			},
			wantErr:   errContinue,
			expectLog: false,
		},
		{
			name:        "change less than 1%",
			startTime:   time.Now().Add(-10 * time.Second),
			done:        50,
			total:       100,
			previousVal: 0.495, // only 0.5% change
			getMetrics: func() *models.Metrics {
				return &models.Metrics{}
			},
			wantErr:   errContinue,
			expectLog: false,
		},
		{
			name:        "valid progress with metrics",
			startTime:   time.Now().Add(-10 * time.Second),
			done:        50,
			total:       100,
			previousVal: 0.4,
			getMetrics: func() *models.Metrics {
				return &models.Metrics{
					RecordsPerSecond:       1000,
					KilobytesPerSecond:     1024,
					PipelineReadQueueSize:  10,
					PipelineWriteQueueSize: 5,
				}
			},
			wantErr:   nil,
			expectLog: true,
		},
		{
			name:        "valid progress with zero RPS",
			startTime:   time.Now().Add(-10 * time.Second),
			done:        70,
			total:       100,
			previousVal: 0.6,
			getMetrics: func() *models.Metrics {
				return &models.Metrics{
					RecordsPerSecond:   0,
					KilobytesPerSecond: 1000,
				}
			},
			wantErr:   nil,
			expectLog: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

			pct, err := printEstimate(tt.startTime, tt.done, tt.total, tt.previousVal, tt.getMetrics, logger)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)

				expectedPct := tt.done / tt.total
				assert.InDelta(t, expectedPct, pct, 0.01)
			}

			if tt.expectLog {
				assert.Contains(t, buf.String(), "progress")
			} else {
				assert.NotContains(t, buf.String(), "progress")
			}
		})
	}
}

func TestPrintBackupEstimateExtended(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupStats     func() *models.BackupStats
		getMetrics     func() *models.Metrics
		contextTimeout time.Duration
		expectTimeout  bool
	}{
		{
			name: "normal operation with progress",
			setupStats: func() *models.BackupStats {
				stats := models.NewBackupStats()
				stats.Start()
				stats.TotalRecords.Add(100)
				stats.ReadRecords.Add(50)

				return stats
			},
			getMetrics: func() *models.Metrics {
				return &models.Metrics{
					KilobytesPerSecond: 1000,
					RecordsPerSecond:   1000,
				}
			},
			contextTimeout: 100 * time.Millisecond,
			expectTimeout:  true,
		},
		{
			name: "zero total records",
			setupStats: func() *models.BackupStats {
				stats := models.NewBackupStats()
				stats.Start()
				stats.TotalRecords.Add(0)
				stats.ReadRecords.Add(0)

				return stats
			},
			getMetrics: func() *models.Metrics {
				return &models.Metrics{}
			},
			contextTimeout: 50 * time.Millisecond,
			expectTimeout:  true,
		},
		{
			name: "completion scenario",
			setupStats: func() *models.BackupStats {
				stats := models.NewBackupStats()
				stats.Start()
				stats.TotalRecords.Add(100)
				stats.ReadRecords.Add(100) // 100% complete

				return stats
			},
			getMetrics: func() *models.Metrics {
				return &models.Metrics{
					RecordsPerSecond: 1000,
				}
			},
			contextTimeout: 100 * time.Millisecond,
			expectTimeout:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(t.Context(), tt.contextTimeout)
			defer cancel()

			stats := tt.setupStats()
			logger := slog.New(slog.DiscardHandler)

			done := make(chan struct{})

			go func() {
				PrintBackupEstimate(ctx, stats, tt.getMetrics, logger)
				close(done)
			}()

			select {
			case <-done:
				if tt.expectTimeout {
					t.Log("Function completed before timeout (expected)")
				}
			case <-time.After(tt.contextTimeout + 50*time.Millisecond):
				if !tt.expectTimeout {
					t.Fatal("PrintBackupEstimate did not complete in time")
				}
			}
		})
	}
}

func TestPrintRestoreEstimateExtended(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupStats     func() *models.RestoreStats
		getMetrics     func() *models.Metrics
		getSize        func() int64
		contextTimeout time.Duration
		expectTimeout  bool
	}{
		{
			name: "normal operation with progress",
			setupStats: func() *models.RestoreStats {
				stats := models.NewRestoreStats()
				stats.Start()
				stats.IncrRecordsInserted()
				stats.RecordsSkipped.Add(1)
				stats.IncrRecordsExisted()

				return stats
			},
			getMetrics: func() *models.Metrics {
				return &models.Metrics{
					KilobytesPerSecond: 1000,
					RecordsPerSecond:   1000,
				}
			},
			getSize: func() int64 {
				return 10000
			},
			contextTimeout: 100 * time.Millisecond,
			expectTimeout:  true,
		},
		{
			name: "zero total size",
			setupStats: func() *models.RestoreStats {
				stats := models.NewRestoreStats()
				stats.Start()

				return stats
			},
			getMetrics: func() *models.Metrics {
				return &models.Metrics{}
			},
			getSize: func() int64 {
				return 0
			},
			contextTimeout: 50 * time.Millisecond,
			expectTimeout:  true,
		},
		{
			name: "size returns -1 (completion)",
			setupStats: func() *models.RestoreStats {
				stats := models.NewRestoreStats()
				stats.Start()

				return stats
			},
			getMetrics: func() *models.Metrics {
				return &models.Metrics{}
			},
			getSize: func() int64 {
				return -1
			},
			contextTimeout: 100 * time.Millisecond,
			expectTimeout:  false,
		},
		{
			name: "zero bytes read",
			setupStats: func() *models.RestoreStats {
				stats := models.NewRestoreStats()
				stats.Start()
				// Don't increment any bytes read
				return stats
			},
			getMetrics: func() *models.Metrics {
				return &models.Metrics{}
			},
			getSize: func() int64 {
				return 10000
			},
			contextTimeout: 50 * time.Millisecond,
			expectTimeout:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(t.Context(), tt.contextTimeout)
			defer cancel()

			stats := tt.setupStats()
			logger := slog.New(slog.DiscardHandler)

			done := make(chan struct{})

			go func() {
				PrintRestoreEstimate(ctx, stats, tt.getMetrics, tt.getSize, logger)
				close(done)
			}()

			select {
			case <-done:
				if tt.expectTimeout {
					t.Log("Function completed before timeout (expected)")
				}
			case <-time.After(tt.contextTimeout + 50*time.Millisecond):
				if !tt.expectTimeout {
					t.Fatal("PrintRestoreEstimate did not complete in time")
				}
			}
		})
	}
}

func TestPrintFilesNumberContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())

	getNumber := func() int64 {
		return 0 // Always return 0 to keep looping
	}

	logger := slog.New(slog.DiscardHandler)

	done := make(chan struct{})

	go func() {
		PrintFilesNumber(ctx, getNumber, logger)
		close(done)
	}()

	// Cancel context after a short delay
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Function should exit due to context cancellation
	case <-time.After(200 * time.Millisecond):
		t.Fatal("PrintFilesNumber did not exit after context cancellation")
	}
}

func TestPrintFilesNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		getNumber      func() int64
		contextTimeout time.Duration
		expectLog      bool
		expectTimeout  bool
	}{
		{
			name: "normal operation with files found",
			getNumber: func() int64 {
				return 42
			},
			contextTimeout: 200 * time.Millisecond,
			expectLog:      true,
			expectTimeout:  false,
		},
		{
			name: "zero files found",
			getNumber: func() int64 {
				return 0
			},
			contextTimeout: 150 * time.Millisecond,
			expectLog:      false,
			expectTimeout:  true,
		},
		{
			name: "completion signal (-1)",
			getNumber: func() int64 {
				return -1
			},
			contextTimeout: 200 * time.Millisecond,
			expectLog:      false,
			expectTimeout:  false,
		},
		{
			name: "files found after delay",
			getNumber: func() int64 {
				time.Sleep(50 * time.Millisecond)
				return 10
			},
			contextTimeout: 200 * time.Millisecond,
			expectLog:      true,
			expectTimeout:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(t.Context(), tt.contextTimeout)
			defer cancel()

			var buf bytes.Buffer

			logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

			done := make(chan struct{})

			go func() {
				PrintFilesNumber(ctx, tt.getNumber, logger)
				close(done)
			}()

			select {
			case <-done:
				if tt.expectTimeout {
					t.Log("Function completed before timeout (expected)")
				}
			case <-time.After(tt.contextTimeout + 50*time.Millisecond):
				if !tt.expectTimeout {
					t.Fatal("PrintFilesNumber did not complete in time")
				}
			}

			if tt.expectLog {
				assert.Contains(t, buf.String(), "found backup files")
				assert.Contains(t, buf.String(), "number")
			} else {
				assert.NotContains(t, buf.String(), "found backup files")
			}
		})
	}
}
