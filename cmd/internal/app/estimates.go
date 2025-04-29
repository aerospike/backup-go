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

package app

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/aerospike/backup-go/models"
)

var (
	errContinue = errors.New("continue")
	errBrake    = errors.New("brake")
)

// printBackupEstimate prints the backup progress.
// The progress is printed every second.
// The progress is printed only when the total records is greater than 0.
// The progress is printed only when the read records is greater than 0.
func printBackupEstimate(
	ctx context.Context,
	stats *models.BackupStats,
	getMetrics func() *models.Metrics,
	logger *slog.Logger,
) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var previousVal float64

	for {
		select {
		case <-ticker.C:
			totalRecords := stats.TotalRecords.Load()
			if totalRecords == 0 {
				// Wait until we calculate total records.
				continue
			}

			done := stats.GetReadRecords()

			pct, err := printEstimate(stats.StartTime, float64(done), float64(totalRecords), previousVal, getMetrics, logger)

			switch {
			case errors.Is(err, errBrake):
				break
			case errors.Is(err, errContinue):
				continue
			default:
				previousVal = pct
			}

		case <-ctx.Done():
			return
		}
	}
}

// printRestoreEstimate prints the restore progress.
// The progress is printed every second.
// The progress is printed only when the total file size is greater than 0.
// The progress is printed only when the written number of bytes is greater than 0.
func printRestoreEstimate(
	ctx context.Context,
	stats *models.RestoreStats,
	getMetrics func() *models.Metrics,
	getSize func() int64,
	logger *slog.Logger,
) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var previousVal float64

	for {
		select {
		case <-ticker.C:
			totalSize := getSize()
			if totalSize == 0 {
				continue
			}

			done := stats.GetTotalBytesRead()
			if done == 0 {
				continue
			}

			pct, err := printEstimate(stats.StartTime, float64(done), float64(totalSize), previousVal, getMetrics, logger)

			switch {
			case errors.Is(err, errBrake):
				break
			case errors.Is(err, errContinue):
				continue
			default:
				previousVal = pct
			}

		case <-ctx.Done():
			return
		}
	}
}

func printEstimate(
	startTime time.Time,
	done,
	total,
	previousVal float64,
	getMetrics func() *models.Metrics,
	logger *slog.Logger,
) (float64, error) {
	percentage := done / total
	estimatedEndTime := calculateEstimatedEndTime(startTime, percentage)

	switch {
	case percentage >= 1:
		// Exit after 100%.
		return 0, errBrake
	case percentage*100 < 1:
		// Start printing only when we have something.
		return 0, errContinue
	case percentage-previousVal < 0.01:
		// if less than 1% then don't print anything.
		return 0, errContinue
	}

	var (
		rps, kbps, recSize uint64
	)

	metrics := getMetrics()
	if metrics != nil {
		rps = metrics.RecordsPerSecond
		kbps = metrics.KilobytesPerSecond
		// Reformating record size for pretty printing to avoid printing 1024.000000000 bytes.
		if rps > 0 {
			recSize = uint64(float64(kbps) / float64(rps) * 1024)
		}
	}

	logger.Info("progress",
		slog.Uint64("pct", uint64(percentage*100)),
		// Formatting the remaining time to milliseconds to avoid printing 0.000000000 seconds.
		slog.String("remaining", estimatedEndTime.Round(time.Millisecond).String()),
		slog.Uint64("rec/s", rps),
		slog.Uint64("KiB/s", kbps),
		slog.Uint64("B/rec", recSize),
	)

	return percentage, nil
}

func calculateEstimatedEndTime(startTime time.Time, percentDone float64) time.Duration {
	if percentDone < 0.01 {
		return time.Duration(0)
	}

	elapsed := time.Since(startTime)
	totalTime := time.Duration(float64(elapsed) / percentDone)
	result := totalTime - elapsed

	if result < 0 {
		return time.Duration(0)
	}

	return result
}

// printFilesNumber prints the number of files.
func printFilesNumber(
	ctx context.Context,
	getNUmber func() int64,
	fileTypes string,
	logger *slog.Logger,
) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			num := getNUmber()
			if num == 0 {
				continue
			}

			logger.Info("found "+fileTypes+" files", slog.Int64("number", num))

			return
		case <-ctx.Done():
			return
		}
	}
}
