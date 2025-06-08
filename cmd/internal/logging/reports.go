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
	"fmt"
	"log/slog"
	"strings"
	"time"

	bModels "github.com/aerospike/backup-go/models"
)

const (
	headerBackupReport     = "Backup report"
	headerRestoreReport    = "Restore report"
	headerEstimateReport   = "Estimate report"
	headerValidationReport = "Validation report"
)

// ReportBackup prints the backup report.
// if isJSON is true, it prints the report in JSON format, but logger must be passed
func ReportBackup(stats *bModels.BackupStats, isXdr, isJSON bool, logger *slog.Logger) {
	if isJSON {
		logBackupReport(stats, isXdr, logger)
		return
	}

	printBackupReport(stats, isXdr)
}

func printBackupReport(stats *bModels.BackupStats, isXdr bool) {
	fmt.Println(headerBackupReport)
	fmt.Println(strings.Repeat("-", len(headerBackupReport)))

	printMetric("Start Time", stats.StartTime.Format(time.RFC1123))
	printMetric("Duration", stats.GetDuration())

	fmt.Println()

	recordsMetric := "Records Read"
	if isXdr {
		recordsMetric = "Records Received"
	}

	printMetric(recordsMetric, stats.GetReadRecords())

	printMetric("sIndex Read", stats.GetSIndexes())
	printMetric("UDFs Read", stats.GetUDFs())

	fmt.Println()

	printMetric("Bytes Written", stats.GetBytesWritten())
	printMetric("Total Records", stats.TotalRecords.Load())
	printMetric("Files Written", stats.GetFileCount())
}

func logBackupReport(stats *bModels.BackupStats, isXdr bool, logger *slog.Logger) {
	recordsMetric := "records_read"
	if isXdr {
		recordsMetric = "records_received"
	}

	logger.Info(strings.ToLower(headerBackupReport),
		slog.Time("start_time", stats.StartTime),
		slog.Duration("duration", stats.GetDuration()),
		slog.Uint64(recordsMetric, stats.GetReadRecords()),
		slog.Uint64("s_index_read", uint64(stats.GetSIndexes())),
		slog.Uint64("udf_read", uint64(stats.GetUDFs())),
		slog.Uint64("bytes_written", stats.GetBytesWritten()),
		slog.Uint64("total_records", stats.TotalRecords.Load()),
		slog.Uint64("files_written", stats.GetFileCount()),
	)
}

// ReportRestore prints the restore report.
// if isJSON is true, it prints the report in JSON format, but logger must be passed
func ReportRestore(stats *bModels.RestoreStats, isValidation, isJSON bool, logger *slog.Logger) {
	if isJSON {
		logRestoreReport(stats, logger, isValidation)
		return
	}

	printRestoreReport(stats, isValidation)
}

func printRestoreReport(stats *bModels.RestoreStats, isValidation bool) {
	header := headerRestoreReport
	if isValidation {
		header = headerValidationReport
	}

	fmt.Println(header)
	fmt.Println(strings.Repeat("-", len(header)))

	printMetric("Start Time", stats.StartTime.Format(time.RFC1123))
	printMetric("Duration", stats.GetDuration())

	fmt.Println()

	printMetric("Records Read", stats.GetReadRecords())
	printMetric("sIndex Read", stats.GetSIndexes())
	printMetric("UDFs Read", stats.GetUDFs())

	fmt.Println()

	// For validation, we don't print the following metrics'
	if !isValidation {
		printMetric("Expired Records", stats.GetRecordsExpired())
		printMetric("Skipped Records", stats.GetRecordsSkipped())
		printMetric("Ignored Records", stats.GetRecordsIgnored())
		printMetric("Fresher Records", stats.GetRecordsFresher())
		printMetric("Existed Records", stats.GetRecordsExisted())

		fmt.Println()

		printMetric("Inserted Records", stats.GetRecordsInserted())
		printMetric("In Doubt Errors", stats.GetErrorsInDoubt())
	}

	if stats.GetTotalBytesRead() > 0 {
		// At the moment, we don't count the size of records.
		printMetric("Total Bytes Read", stats.GetTotalBytesRead())
	}
}

func logRestoreReport(stats *bModels.RestoreStats, logger *slog.Logger, isValidation bool) {
	header := strings.ToLower(headerRestoreReport)
	if isValidation {
		header = strings.ToLower(headerValidationReport)
	}

	logAttr := make([]any, 0)
	logAttr = append(logAttr,
		slog.Time("start_time", stats.StartTime),
		slog.Duration("duration", stats.GetDuration()),
		slog.Uint64("records_read", stats.GetReadRecords()),
		slog.Uint64("s_index_read", uint64(stats.GetSIndexes())),
		slog.Uint64("udf_read", uint64(stats.GetUDFs())),
	)

	if !isValidation {
		logAttr = append(logAttr,
			slog.Uint64("expired_records", stats.GetRecordsExpired()),
			slog.Uint64("skipped_records", stats.GetRecordsSkipped()),
			slog.Uint64("ignored_records", stats.GetRecordsIgnored()),
			slog.Uint64("fresher_records", stats.GetRecordsFresher()),
			slog.Uint64("existed_records", stats.GetRecordsExisted()),
			slog.Uint64("inserted_records", stats.GetRecordsInserted()),
			slog.Uint64("in_doubt_errors", stats.GetErrorsInDoubt()),
		)
	}

	logAttr = append(logAttr,
		slog.Uint64("total_bytes_read", stats.GetTotalBytesRead()),
	)

	logger.Info(header, logAttr...)
}

// ReportEstimate prints the estimate report.
// if isJSON is true, it prints the report in JSON format, but logger must be passed.
// estimate is the size of the backup file in bytes.
func ReportEstimate(estimate uint64, isJSON bool, logger *slog.Logger) {
	if isJSON {
		logEstimateReport(estimate, logger)
		return
	}

	printEstimateReport(estimate)
}

func printEstimateReport(estimate uint64) {
	fmt.Println(headerEstimateReport)
	fmt.Println(strings.Repeat("-", len(headerEstimateReport)))

	printMetric("File size (bytes)", estimate)
}

func logEstimateReport(estimate uint64, logger *slog.Logger) {
	logger.Info(strings.ToLower(headerEstimateReport),
		slog.Uint64("file_size_bytes", estimate),
	)
}

func printMetric(key string, value any) {
	fmt.Printf("%s%v\n", indent(key), value)
}

func indent(key string) string {
	return fmt.Sprintf("%s:%s", key, strings.Repeat(" ", 21-len(key)))
}
