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

func printBackupReport(stats, xdrStats *bModels.BackupStats) {
	fmt.Println("Backup Report")
	fmt.Println("--------------")
	fmt.Printf("Start Time:           %s\n", stats.StartTime.Format(time.RFC1123))
	fmt.Printf("Duration:             %s\n", stats.GetDuration())

	fmt.Println()

	fmt.Printf("Records Read:         %d\n", stats.GetReadRecords())

	var bw, fw uint64
	if xdrStats != nil {
		bw = xdrStats.GetBytesWritten()
		fw = xdrStats.GetFileCount()
		fmt.Printf("Records Received:     %d\n", xdrStats.GetReadRecords())
	}

	fmt.Printf("sIndex Read:          %d\n", stats.GetSIndexes())
	fmt.Printf("UDFs Read:            %d\n", stats.GetUDFs())

	fmt.Println()

	fmt.Printf("Bytes Written:        %d bytes\n", stats.GetBytesWritten()+bw)
	fmt.Printf("Total Records:        %d\n", stats.TotalRecords)
	fmt.Printf("Files Written:        %d\n", stats.GetFileCount()+fw)
}

func printRestoreReport(asbStats, asbxStats *bModels.RestoreStats) {
	fmt.Println("Restore Report")
	fmt.Println("--------------")
	fmt.Printf("Start Time:           %s\n", asbStats.StartTime.Format(time.RFC1123))
	fmt.Printf("Duration:             %s\n", asbStats.GetDuration())

	fmt.Println()

	var rr, ir, ri, br uint64
	if asbxStats != nil {
		rr = asbxStats.GetReadRecords()
		ir = asbStats.GetRecordsIgnored()
		ri = asbxStats.GetRecordsInserted()
		br = asbxStats.GetTotalBytesRead()
	}

	fmt.Printf("Records Read:         %d\n", asbStats.GetReadRecords()+rr)
	fmt.Printf("sIndex Read:          %d\n", asbStats.GetSIndexes())
	fmt.Printf("UDFs Read:            %d\n", asbStats.GetUDFs())
	fmt.Printf("Bytes Written:        %d bytes\n", asbStats.GetBytesWritten())

	fmt.Println()

	fmt.Printf("Expired Records:      %d\n", asbStats.GetRecordsExpired())
	fmt.Printf("Skipped Records:      %d\n", asbStats.GetRecordsSkipped())
	fmt.Printf("Ignored Records:      %d\n", asbStats.GetRecordsIgnored()+ir)
	fmt.Printf("Fresher Records:     %d\n", asbStats.GetRecordsFresher())
	fmt.Printf("Existed Records:      %d\n", asbStats.GetRecordsExisted())

	fmt.Println()

	fmt.Printf("Inserted Records:     %d\n", asbStats.GetRecordsInserted()+ri)
	fmt.Printf("Total Bytes Read:     %d\n", asbStats.GetTotalBytesRead()+br)
}

func printEstimateReport(estimate uint64) {
	fmt.Println("Estimate Report")
	fmt.Println("--------------")
	fmt.Printf("File size: %d bytes\n", estimate)
}
