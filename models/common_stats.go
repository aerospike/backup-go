package models

import (
	"sync/atomic"
	"time"
)

type commonStats struct {
	start    time.Time
	duration time.Duration
	// number of records read from source, before any filtering.
	RecordsTotal atomic.Uint64
	// The number of successfully created secondary indexes.
	sIndexes atomic.Uint32
	// The number of successfully stored UDF files.
	uDFs atomic.Uint32
	// The total number of bytes written to the destination.
	totalBytesWritten atomic.Uint64
}

func (s *commonStats) GetRecordsTotal() uint64 {
	return s.RecordsTotal.Load()
}

func (s *commonStats) GetTotalBytesWritten() uint64 {
	return s.totalBytesWritten.Load()
}

func (s *commonStats) GetSIndexes() uint32 {
	return s.sIndexes.Load()
}

func (s *commonStats) GetUDFs() uint32 {
	return s.uDFs.Load()
}

func (s *commonStats) AddTotalBytesWritten(num uint64) {
	s.totalBytesWritten.Add(num)
}

func (s *commonStats) AddSIndexes(num uint32) {
	s.sIndexes.Add(num)
}

func (s *commonStats) AddUDFs(num uint32) {
	s.uDFs.Add(num)
}

func (s *commonStats) Start() {
	s.start = time.Now()
}

func (s *commonStats) Stop() {
	if s.duration == 0 {
		s.duration = time.Since(s.start)
	}
}

func (s *commonStats) GetDuration() time.Duration {
	return s.duration
}
