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
	"fmt"
	"strings"
	"time"

	bModels "github.com/aerospike/backup-go/models"
)

const (
	headerBackupReport   = "Backup report"
	headerRestoreReport  = "Restore report"
	headerEstimateReport = "Estimate report"
)

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

func printRestoreReport(asbStats *bModels.RestoreStats) {
	fmt.Println(headerRestoreReport)
	fmt.Println(strings.Repeat("-", len(headerRestoreReport)))

	printMetric("Start Time", asbStats.StartTime.Format(time.RFC1123))
	printMetric("Duration", asbStats.GetDuration())

	fmt.Println()

	printMetric("Records Read", asbStats.GetReadRecords())
	printMetric("sIndex Read", asbStats.GetSIndexes())
	printMetric("UDFs Read", asbStats.GetUDFs())

	fmt.Println()

	printMetric("Expired Records", asbStats.GetRecordsExpired())
	printMetric("Skipped Records", asbStats.GetRecordsSkipped())
	printMetric("Ignored Records", asbStats.GetRecordsIgnored())
	printMetric("Fresher Records", asbStats.GetRecordsFresher())
	printMetric("Existed Records", asbStats.GetRecordsExisted())

	fmt.Println()

	printMetric("Inserted Records", asbStats.GetRecordsInserted())
	printMetric("In Doubt Errors", asbStats.GetErrorsInDoubt())

	if asbStats.GetTotalBytesRead() > 0 {
		// At the moment, we don't count the size of records.
		printMetric("Total Bytes Read", asbStats.GetTotalBytesRead())
	}
}

func printEstimateReport(estimate uint64) {
	fmt.Println(headerEstimateReport)
	fmt.Println(strings.Repeat("-", len(headerEstimateReport)))

	printMetric("File size (bytes)", estimate)
}

func printMetric(key string, value any) {
	fmt.Printf("%s%v\n", indent(key), value)
}

func indent(key string) string {
	return fmt.Sprintf("%s:%s", key, strings.Repeat(" ", 21-len(key)))
}
