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

package estimates

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"time"

	"github.com/aerospike/backup-go/models"
)

const (
	// TargetPrintInterval is the interval between progress log lines that the dynamic
	// threshold aims for. It is also the tick rate of the estimate printers.
	TargetPrintInterval = 5 * time.Second
	// EstimateWarmup is the time before the first estimate is produced, so that it is
	// calculated from enough data to be meaningful.
	EstimateWarmup = 5 * time.Second
	// Limits of a dynamic threshold.
	maxThreshold = 0.01
	minThreshold = 0.0001
)

var (
	errContinue = errors.New("continue")
	errBrake    = errors.New("brake")
)

// PrintBackupEstimate prints the backup progress.
// The progress is printed every second.
// The progress is printed only when the total records is greater than 0.
// The progress is printed only when the read records is greater than 0.
func PrintBackupEstimate(
	ctx context.Context,
	stats *models.BackupStats,
	getMetrics func() *models.Metrics,
	logger *slog.Logger,
) {
	ticker := time.NewTicker(TargetPrintInterval)
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
				return
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

// PrintRestoreEstimate prints the restore progress.
// The progress is printed every second.
// The progress is printed only when the total file size is greater than 0.
// The progress is printed only when the written number of bytes is greater than 0.
func PrintRestoreEstimate(
	ctx context.Context,
	stats *models.RestoreStats,
	getMetrics func() *models.Metrics,
	getSize func() int64,
	logger *slog.Logger,
) {
	ticker := time.NewTicker(TargetPrintInterval)
	defer ticker.Stop()

	var previousVal float64

	for {
		select {
		case <-ticker.C:
			totalSize := getSize()
			switch totalSize {
			case -1:
				return
			case 0:
				continue
			}

			done := stats.GetTotalBytesRead()
			if done == 0 {
				continue
			}

			pct, err := printEstimate(stats.StartTime, float64(done), float64(totalSize), previousVal, getMetrics, logger)

			switch {
			case errors.Is(err, errBrake):
				return
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
	ratio := done / total
	elapsed := time.Since(startTime)
	estimatedEndTime := RemainingTime(elapsed, ratio)

	switch {
	case ratio >= 1:
		return 0, errBrake
	case ratio*100 < 0.01: // It duplicates progressThreshold but protect from division by zero.
		return 0, errContinue
	}

	// Calculate dynamically how many times we should skip ticker before printing.
	// Not to flood the logs.
	threshold := ProgressThreshold(elapsed, ratio)
	if ratio-previousVal < threshold {
		return 0, errContinue
	}

	var rps, kbps, recSize uint64

	metrics := getMetrics()
	if metrics != nil {
		rps = metrics.RecordsPerSecond

		kbps = metrics.KilobytesPerSecond
		if rps > 0 {
			recSize = uint64(float64(kbps) / float64(rps) * 1024)
		}

		logger.Debug("pipe metrics",
			slog.Int("read", metrics.PipelineReadQueueSize),
			slog.Int("write", metrics.PipelineWriteQueueSize),
		)
	}

	logger.Info("progress",
		slog.Float64("pct", math.Round(ratio*10000)/100),
		slog.String("remaining", estimatedEndTime.Round(time.Millisecond).String()),
		slog.Uint64("rec/s", rps),
		slog.Uint64("kiB/s", kbps),
		slog.Uint64("b/rec", recSize),
	)

	return ratio, nil
}

// ProgressThreshold returns the minimum progress delta, as a ratio in [0, 1], that is
// worth reporting at the current point of a job.
//
// The value is derived from the total duration projected from elapsed and ratio, so that
// lines are emitted roughly every TargetPrintInterval regardless of how long the job runs:
// a multi-hour job does not flood the log, and a short one still reports often enough.
// The result is clamped to [minThreshold, maxThreshold], and is maxThreshold while elapsed
// is below EstimateWarmup or ratio is not positive.
//
// elapsed is the time since the job started, ratio is the completed fraction in [0, 1].
func ProgressThreshold(elapsed time.Duration, ratio float64) float64 {
	// While we don't have enough data, use the default threshold of 1%.
	if elapsed < EstimateWarmup || ratio <= 0 {
		return maxThreshold
	}

	totalDuration := float64(elapsed) / ratio
	threshold := float64(TargetPrintInterval) / totalDuration

	switch {
	case threshold > maxThreshold:
		return maxThreshold
	case threshold < minThreshold:
		return minThreshold
	default:
		return threshold
	}
}

// RemainingTime estimates the time left until a job completes by projecting the average
// rate observed so far over the remaining work.
//
// It returns 0 whenever the estimate is not meaningful: while elapsed is below
// EstimateWarmup, when ratio is not positive, and once the job is complete.
//
// elapsed is the time since the job started, ratio is the completed fraction in [0, 1].
func RemainingTime(elapsed time.Duration, ratio float64) time.Duration {
	if elapsed < EstimateWarmup || ratio <= 0 {
		return 0
	}

	totalTime := time.Duration(float64(elapsed) / ratio)

	remaining := totalTime - elapsed
	if remaining < 0 {
		return 0
	}

	return remaining
}

// PrintFilesNumber prints the number of files.
func PrintFilesNumber(
	ctx context.Context,
	getNumber func() int64,
	logger *slog.Logger,
) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			num := getNumber()
			switch num {
			case -1:
				return
			case 0:
				continue
			}

			logger.Info("found backup files", slog.Int64("number", num))

			return
		case <-ctx.Done():
			return
		}
	}
}
