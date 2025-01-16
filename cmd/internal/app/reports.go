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
	"time"

	bModels "github.com/aerospike/backup-go/models"
)

const (
	reportHeaderBackup     = "Backup Report"
	reportHeaderBackupXDR  = "XDR Backup Report"
	reportHeaderRestore    = "ASB Restore Report"
	reportHeaderRestoreXDR = "ASBX Restore Report"
)

func printBackupReport(header string, stats *bModels.BackupStats) {
	fmt.Println(header)
	fmt.Println("--------------")
	fmt.Printf("Start Time:           %s\n", stats.StartTime.Format(time.RFC1123))
	fmt.Printf("Duration:             %s\n", stats.GetDuration())
	fmt.Printf("Records Read:         %d\n", stats.GetReadRecords())
	fmt.Printf("sIndex Read:          %d\n", stats.GetSIndexes())
	fmt.Printf("UDFs Read:            %d\n", stats.GetUDFs())
	fmt.Printf("Bytes Written:        %d bytes\n", stats.GetBytesWritten())

	fmt.Printf("Total Records:        %d\n", stats.TotalRecords)
	fmt.Printf("Files Written:        %d\n", stats.GetFileCount())
}

func printRestoreReport(header string, stats *bModels.RestoreStats) {
	fmt.Println(header)
	fmt.Println("--------------")
	fmt.Printf("Start Time:           %s\n", stats.StartTime.Format(time.RFC1123))
	fmt.Printf("Duration:             %s\n", stats.GetDuration())
	fmt.Printf("Records Read:         %d\n", stats.GetReadRecords())
	fmt.Printf("sIndex Read:          %d\n", stats.GetSIndexes())
	fmt.Printf("UDFs Read:            %d\n", stats.GetUDFs())
	fmt.Printf("Bytes Written:        %d bytes\n", stats.GetBytesWritten())

	fmt.Printf("Expired Records:      %d\n", stats.GetRecordsExpired())
	fmt.Printf("Skipped Records:      %d\n", stats.GetRecordsSkipped())
	fmt.Printf("Ignored Records:      %d\n", stats.GetRecordsIgnored())
	fmt.Printf("Fresher Records:     %d\n", stats.GetRecordsFresher())
	fmt.Printf("Existed Records:      %d\n", stats.GetRecordsExisted())
	fmt.Printf("Inserted Records:     %d\n", stats.GetRecordsInserted())
	fmt.Printf("Total Bytes Read:     %d\n", stats.GetTotalBytesRead())
}

func printEstimateReport(estimate uint64) {
	fmt.Println("Estimate Report")
	fmt.Println("--------------")
	fmt.Printf("File size: %d bytes\n", estimate)
}
