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
	"log/slog"
	"time"

	"github.com/aerospike/backup-go/models"
)

func printBackupEstimate(ctx context.Context, stats *models.BackupStats, logger *slog.Logger) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if stats.TotalRecords == 0 {
				// Wait until we calculate total records.
				continue
			}

			done := stats.GetReadRecords()
			percentage := float64(done) / float64(stats.TotalRecords)
			estimatedEndTime := calculateEstimatedEndTime(stats.StartTime, percentage)

			switch {
			case percentage >= 1:
				// Exit after 100%.
				return
			case percentage*100 < 1:
				// Start printing only when we have somthing.
				continue
			}

			logger.Info("complete", slog.Int64("%", int64(percentage*100)))
			logger.Info("estimates", slog.Duration("remains", estimatedEndTime))
		case <-ctx.Done():
			return
		}
	}
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

func printRestoreEstimate(ctx context.Context, stats *models.RestoreStats, logger *slog.Logger) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			done := stats.GetRecordsInserted() + stats.GetRecordsSkipped() + stats.GetRecordsExisted() +
				stats.GetRecordsExpired() + stats.GetRecordsFresher()

			// We don't know total records count, so can't calculate estimates.
			if done > 0 {
				logger.Info("complete", slog.Uint64("count", done))
			}

		case <-ctx.Done():
			return
		}
	}
}
