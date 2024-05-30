package models

import (
	"sync/atomic"
)

// RestoreStats stores the stats of a restore from reader job
type RestoreStats struct {
	commonStats
	// The number of records dropped because they were expired.
	RecordsExpired atomic.Uint64
	// The number of records dropped because they didn't contain any of the
	// selected bins or didn't belong to any of the selected sets.
	RecordsSkipped atomic.Uint64
	// The number of records dropped because the database already contained the
	// records with a higher generation count.
	recordsFresher atomic.Uint64
	// The number of records dropped because they already existed in the
	// database.
	recordsExisted atomic.Uint64
	// The number of successfully restored records.
	recordsInserted atomic.Uint64
	// Total number of bytes read from source.
	TotalBytesRead atomic.Uint64
}

func (rs *RestoreStats) GetRecordsExpired() uint64 {
	return rs.RecordsExpired.Load()
}

func (rs *RestoreStats) GetRecordsSkipped() uint64 {
	return rs.RecordsSkipped.Load()
}

func (rs *RestoreStats) GetRecordsFresher() uint64 {
	return rs.recordsFresher.Load()
}

func (rs *RestoreStats) IncrRecordsFresher() {
	rs.recordsFresher.Add(1)
}

func (rs *RestoreStats) GetRecordsExisted() uint64 {
	return rs.recordsExisted.Load()
}

func (rs *RestoreStats) IncrRecordsExisted() {
	rs.recordsExisted.Add(1)
}

func (rs *RestoreStats) GetRecordsInserted() uint64 {
	return rs.recordsInserted.Load()
}

func (rs *RestoreStats) IncrRecordsInserted() {
	rs.recordsInserted.Add(1)
}

func (rs *RestoreStats) GetTotalBytesRead() uint64 {
	return rs.TotalBytesRead.Load()
}
