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
)

func printBackupReport(stats, xdrStats *bModels.BackupStats) {
	fmt.Println(headerBackupReport)
	fmt.Println(strings.Repeat("-", len(headerBackupReport)))

	printMetric("Start Time", stats.StartTime.Format(time.RFC1123))

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

	printMetric("Duration", dur)

	fmt.Println()

	switch {
	case xdrStats != nil:
		printMetric("Records Received", xdrStats.GetReadRecords())
	default:
		printMetric("Records Read", stats.GetReadRecords())
	}

	printMetric("sIndex Read", stats.GetSIndexes())
	printMetric("UDFs Read", stats.GetUDFs())

	fmt.Println()

	printMetric("Bytes Written", stats.GetBytesWritten()+bw)
	printMetric("Total Records", stats.TotalRecords+tr)
	printMetric("Files Written", stats.GetFileCount()+fw)
}

func printRestoreReport(asbStats, asbxStats *bModels.RestoreStats) {
	fmt.Println(headerRestoreReport)
	fmt.Println(strings.Repeat("-", len(headerRestoreReport)))

	// In case of asbx restore, asbStats will be nil, so we swap vars to print a report from asbx values.
	if asbStats == nil && asbxStats != nil {
		asbStats, asbxStats = asbxStats, nil
	}

	dur := asbStats.GetDuration()

	var rr, ir, ri, rf, re uint64

	if asbxStats != nil {
		rr = asbxStats.GetReadRecords()
		ir = asbxStats.GetRecordsIgnored()
		ri = asbxStats.GetRecordsInserted()
		rf = asbxStats.GetRecordsFresher()
		re = asbxStats.GetRecordsExisted()

		if asbxStats.GetDuration() > dur {
			dur = asbxStats.GetDuration()
		}
	}

	printMetric("Start Time", asbStats.StartTime.Format(time.RFC1123))
	printMetric("Duration", dur)

	fmt.Println()

	printMetric("Records Read", asbStats.GetReadRecords()+rr)
	printMetric("sIndex Read", asbStats.GetSIndexes())
	printMetric("UDFs Read", asbStats.GetUDFs())

	fmt.Println()

	printMetric("Expired Records", asbStats.GetRecordsExpired())
	printMetric("Skipped Records", asbStats.GetRecordsSkipped())
	printMetric("Ignored Records", asbStats.GetRecordsIgnored()+ir)
	printMetric("Fresher Records", asbStats.GetRecordsFresher()+rf)
	printMetric("Existed Records", asbStats.GetRecordsExisted()+re)

	fmt.Println()

	printMetric("Inserted Records", asbStats.GetRecordsInserted()+ri)

	if asbxStats == nil {
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
