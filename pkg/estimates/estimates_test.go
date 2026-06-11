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
	t.Parallel()

	// startTime is computed inside the subtest (not in the struct) to keep the
	// gap between "time.Now() in the test" and "time.Since() inside the function"
	// in the microsecond range. Otherwise time drift is amplified by 1/ratio.
	tests := []struct {
		name      string
		elapsed   time.Duration
		ratio     float64
		expected  time.Duration
		tolerance time.Duration
	}{
		{
			name:      "elapsed less than warmup returns zero",
			elapsed:   2 * time.Second, // < estimateWarmup (5s)
			ratio:     0.5,
			expected:  0,
			tolerance: 100 * time.Millisecond,
		},
		{
			name:      "zero ratio returns zero",
			elapsed:   10 * time.Second,
			ratio:     0,
			expected:  0,
			tolerance: 100 * time.Millisecond,
		},
		{
			name:      "negative ratio returns zero",
			elapsed:   10 * time.Second,
			ratio:     -0.1,
			expected:  0,
			tolerance: 100 * time.Millisecond,
		},
		{
			name:     "10 percent done after 10s",
			elapsed:  10 * time.Second,
			ratio:    0.1,
			expected: 90 * time.Second,
			// drift amplified by 1/0.1 = 10x
			tolerance: 500 * time.Millisecond,
		},
		{
			name:      "50 percent done after 10s",
			elapsed:   10 * time.Second,
			ratio:     0.5,
			expected:  10 * time.Second,
			tolerance: 100 * time.Millisecond,
		},
		{
			name:      "100 percent done returns zero",
			elapsed:   10 * time.Second,
			ratio:     1.0,
			expected:  0,
			tolerance: 100 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			startTime := time.Now().Add(-tt.elapsed)

			result := calculateEstimatedEndTime(startTime, tt.ratio)

			assert.InDelta(t, tt.expected.Milliseconds(), result.Milliseconds(), float64(tt.tolerance.Milliseconds()))
		})
	}
}

func TestProgressThreshold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		elapsed  time.Duration
		ratio    float64
		expected float64
	}{
		{
			name:     "during warmup returns max threshold",
			elapsed:  2 * time.Second, // < estimateWarmup
			ratio:    0.001,
			expected: maxThreshold,
		},
		{
			name:    "short backup clamps to max threshold",
			elapsed: 10 * time.Second,
			// totalDuration = 10s / 0.5 = 20s; threshold = 5s/20s = 0.25 -> clamped to 0.01
			ratio:    0.5,
			expected: maxThreshold,
		},
		{
			name:    "very long backup clamps to min threshold",
			elapsed: 10 * time.Second,
			// totalDuration = 10s / 0.0001 = 100000s; threshold = 5s/100000s = 5e-5 -> clamped to 1e-4
			ratio:    0.0001,
			expected: minThreshold,
		},
		{
			name:    "medium backup returns computed threshold",
			elapsed: 10 * time.Second,
			// totalDuration = 10s / 0.01 = 1000s; threshold = 5s/1000s = 0.005 (within bounds)
			ratio:    0.01,
			expected: 0.005,
		},
		{
			name:    "boundary at max threshold returns max",
			elapsed: 10 * time.Second,
			// totalDuration = 10s / 0.02 = 500s; threshold = 5s/500s = 0.01 = maxThreshold
			ratio:    0.02,
			expected: maxThreshold,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := progressThreshold(tt.elapsed, tt.ratio)
			assert.InDelta(t, tt.expected, got, 1e-9)
		})
	}
}

func TestPrintEstimate(t *testing.T) {
	t.Parallel()

	// elapsed is used instead of startTime to compute startTime inside the subtest,
	// avoiding scheduling-induced time drift in time-sensitive cases (e.g. warmup).
	tests := []struct {
		name        string
		elapsed     time.Duration
		done        float64
		total       float64
		previousVal float64
		getMetrics  func() *models.Metrics
		wantErr     error
		expectLog   bool
	}{
		{
			name:        "completion at 100% returns errBrake",
			elapsed:     10 * time.Second,
			done:        100,
			total:       100,
			previousVal: 0.5,
			getMetrics:  func() *models.Metrics { return &models.Metrics{} },
			wantErr:     errBrake,
			expectLog:   false,
		},
		{
			name:        "ratio below 0.01% guard returns errContinue",
			elapsed:     10 * time.Second,
			done:        0.005, // ratio = 5e-5 = 0.005%, below the 0.01% guard
			total:       100,
			previousVal: 0,
			getMetrics:  func() *models.Metrics { return &models.Metrics{} },
			wantErr:     errContinue,
			expectLog:   false,
		},
		{
			name:    "delta below dynamic threshold returns errContinue",
			elapsed: 10 * time.Second,
			// elapsed=10s, ratio=0.5 -> totalDuration=20s -> threshold clamped to maxThreshold=0.01
			// delta = 0.5 - 0.495 = 0.005 < 0.01
			done:        50,
			total:       100,
			previousVal: 0.495,
			getMetrics:  func() *models.Metrics { return &models.Metrics{} },
			wantErr:     errContinue,
			expectLog:   false,
		},
		{
			name:    "delta above threshold prints with metrics",
			elapsed: 10 * time.Second,
			// delta = 0.5 - 0.4 = 0.1 > 0.01 (clamped threshold)
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
			name: "during warmup uses max threshold",
			// elapsed=2s < warmup, threshold=maxThreshold=0.01
			// delta = 0.5 - 0 = 0.5 > 0.01 -> prints
			elapsed:     2 * time.Second,
			done:        50,
			total:       100,
			previousVal: 0,
			getMetrics: func() *models.Metrics {
				return &models.Metrics{
					RecordsPerSecond:   1000,
					KilobytesPerSecond: 1024,
				}
			},
			wantErr:   nil,
			expectLog: true,
		},
		{
			name:        "zero RPS does not break record size calc",
			elapsed:     10 * time.Second,
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
		{
			name:        "nil metrics still prints progress",
			elapsed:     10 * time.Second,
			done:        50,
			total:       100,
			previousVal: 0.4,
			getMetrics:  func() *models.Metrics { return nil },
			wantErr:     nil,
			expectLog:   true,
		},
		{
			name:    "fine-grained delta passes for long backup",
			elapsed: 1 * time.Hour,
			// elapsed=1h, ratio=0.01 -> totalDuration=100h -> threshold=5s/360000s=1.39e-5, clamped to 1e-4
			// delta = 0.01 - 0.0098 = 0.0002 > 1e-4 -> prints
			done:        1,
			total:       100,
			previousVal: 0.0098,
			getMetrics:  func() *models.Metrics { return &models.Metrics{} },
			wantErr:     nil,
			expectLog:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

			startTime := time.Now().Add(-tt.elapsed)

			pct, err := printEstimate(startTime, tt.done, tt.total, tt.previousVal, tt.getMetrics, logger)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)

				expectedRatio := tt.done / tt.total
				assert.InDelta(t, expectedRatio, pct, 1e-9, "returned value must be the raw ratio (0..1)")
			}

			logged := buf.String()
			if tt.expectLog {
				assert.Contains(t, logged, "progress")
				// pct in the log must be a float with 2-decimal precision (not an integer cast).
				assert.Contains(t, logged, "pct=")
			} else {
				assert.NotContains(t, logged, "progress")
			}
		})
	}
}

// TestPrintEstimate_PctFormat verifies the percentage is rounded to 2 decimal places
// in the log output (not truncated to an integer like the old uint64 cast).
func TestPrintEstimate_PctFormat(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// ratio = 0.5051 -> pct = round(50.51 * 100) / 100 = 50.51
	startTime := time.Now().Add(-10 * time.Second)
	_, err := printEstimate(startTime, 50.51, 100, 0, func() *models.Metrics { return nil }, logger)
	require.NoError(t, err)

	assert.Contains(t, buf.String(), "pct=50.51",
		"percentage must be logged with 2-decimal precision")
}

// TestPrintBackupEstimate verifies the goroutine exits cleanly on context cancellation.
// With ticker == targetPrintInterval (5s), no print happens within the test timeout —
// we are validating the shutdown path, not the print path.
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

// TestPrintRestoreEstimate — same intent as TestPrintBackupEstimate, see comment there.
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
				// Don't increment any bytes read.
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

	// Cancel context after a short delay.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Function should exit due to context cancellation.
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
