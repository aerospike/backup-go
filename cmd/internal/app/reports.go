package app

import (
	"fmt"
	"time"

	bModels "github.com/aerospike/backup-go/models"
)

func printBackupReport(stats *bModels.BackupStats) {
	fmt.Println("Backup Report")
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

func printRestoreReport(stats *bModels.RestoreStats) {
	fmt.Println("Restore Report")
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
	fmt.Printf("Freasher Records:     %d\n", stats.GetRecordsFresher())
	fmt.Printf("Existed Records:      %d\n", stats.GetRecordsExisted())
	fmt.Printf("Inserted Records:     %d\n", stats.GetRecordsInserted())
	fmt.Printf("Total Bytes Read:     %d\n", stats.GetTotalBytesRead())
}

func printEstimateReport(estimate uint64) {
	fmt.Println("Estimate Report")
	fmt.Println("--------------")
	fmt.Printf("File size: %d bytes\n", estimate)
}
