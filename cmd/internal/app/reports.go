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
	headerBackupReport   = "Backup Report"
	headerRestoreReport  = "Restore Report"
	headerEstimateReport = "Estimate Report"

	templateString = "%s%s\n"
	templateInt    = "%s%d\n"
)

func printBackupReport(stats, xdrStats *bModels.BackupStats) {
	fmt.Println(headerBackupReport)
	fmt.Println(strings.Repeat("-", len(headerBackupReport)))

	fmt.Printf(templateString, indent("Start Time"), stats.StartTime.Format(time.RFC1123))

	dur := stats.GetDuration()

	var bw, fw, tr uint64

	if xdrStats != nil {
		bw = xdrStats.GetBytesWritten()
		fw = xdrStats.GetFileCount()
		tr = xdrStats.TotalRecords

		if xdrStats.GetDuration() > dur {
			dur = xdrStats.GetDuration()
		}
	}

	fmt.Printf(templateInt, indent("Duration"), dur)

	fmt.Println()

	switch {
	case xdrStats != nil:
		fmt.Printf(templateInt, indent("Records Received"), xdrStats.GetReadRecords())
	default:
		fmt.Printf(templateInt, indent("Records Read"), stats.GetReadRecords())
	}

	fmt.Printf(templateInt, indent("sIndex Read"), stats.GetSIndexes())
	fmt.Printf(templateInt, indent("UDFs Read"), stats.GetUDFs())

	fmt.Println()

	fmt.Printf(templateInt, indent("Bytes Written"), stats.GetBytesWritten()+bw)
	fmt.Printf(templateInt, indent("Total Records"), stats.TotalRecords+tr)
	fmt.Printf(templateInt, indent("Files Written"), stats.GetFileCount()+fw)
}

func printRestoreReport(asbStats, asbxStats *bModels.RestoreStats) {
	fmt.Println(headerRestoreReport)
	fmt.Println(strings.Repeat("-", len(headerRestoreReport)))

	dur := asbStats.GetDuration()

	var rr, ir, ri, br uint64

	if asbxStats != nil {
		rr = asbxStats.GetReadRecords()
		ir = asbxStats.GetRecordsIgnored()
		ri = asbxStats.GetRecordsInserted()
		br = asbxStats.GetTotalBytesRead()

		if asbxStats.GetDuration() > dur {
			dur = asbxStats.GetDuration()
		}
	}

	fmt.Printf(templateString, indent("Start Time"), asbStats.StartTime.Format(time.RFC1123))
	fmt.Printf(templateInt, indent("Duration"), dur)

	fmt.Println()

	fmt.Printf(templateInt, indent("Records Read"), asbStats.GetReadRecords()+rr)
	fmt.Printf(templateInt, indent("sIndex Read"), asbStats.GetSIndexes())
	fmt.Printf(templateInt, indent("UDFs Read"), asbStats.GetUDFs())

	fmt.Println()

	fmt.Printf(templateInt, indent("Expired Records"), asbStats.GetRecordsExpired())
	fmt.Printf(templateInt, indent("Skipped Records"), asbStats.GetRecordsSkipped())
	fmt.Printf(templateInt, indent("Ignored Records"), asbStats.GetRecordsIgnored()+ir)
	fmt.Printf(templateInt, indent("Fresher Records"), asbStats.GetRecordsFresher())
	fmt.Printf(templateInt, indent("Existed Records"), asbStats.GetRecordsExisted())

	fmt.Println()

	fmt.Printf(templateInt, indent("Inserted Records"), asbStats.GetRecordsInserted()+ri)
	fmt.Printf(templateInt, indent("Total Bytes Read"), asbStats.GetTotalBytesRead()+br)
}

func printEstimateReport(estimate uint64) {
	fmt.Println(headerEstimateReport)
	fmt.Println(strings.Repeat("-", len(headerEstimateReport)))

	fmt.Printf("File size: %d bytes\n", estimate)
}

func indent(key string) string {
	return fmt.Sprintf("%s:%s", key, strings.Repeat(" ", 21-len(key)))
}
