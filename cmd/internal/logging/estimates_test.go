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

package logging

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
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

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	stats := models.NewBackupStats()
	stats.Start()
	stats.TotalRecords.Add(100)
	stats.ReadRecords.Add(50)

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

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

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	stats := models.NewRestoreStats()
	stats.Start()
	stats.IncrRecordsInserted()
	stats.RecordsSkipped.Add(1)
	stats.IncrRecordsExisted()

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

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
