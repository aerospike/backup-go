package models

import (
	"sync/atomic"
)

// BackupStats stores the status of a backup job.
// Stats are updated in realtime by backup jobs.
type BackupStats struct {
	commonStats
	fileCount atomic.Uint64
	// total number of records in database
	TotalRecords uint64
}

func (b *BackupStats) IncFiles() {
	b.fileCount.Add(1)
}

func (b *BackupStats) GetFileCount() uint64 {
	return b.fileCount.Load()
}

func (b *BackupStats) IsEmpty() bool {
	return b.GetUDFs() == 0 &&
		b.GetSIndexes() == 0 &&
		b.GetReadRecords() == 0
}
