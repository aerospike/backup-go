package models

import (
	"sync/atomic"
	"time"
)

type commonStats struct {
	StartTime time.Time
	duration  time.Duration
	// number of records read from source, before any filtering.
	ReadRecords atomic.Uint64
	// The number of successfully created secondary indexes.
	sIndexes atomic.Uint32
	// The number of successfully stored UDF files.
	uDFs atomic.Uint32
	// The number of bytes written to the destination.
	BytesWritten atomic.Uint64
}

func (s *commonStats) GetReadRecords() uint64 {
	return s.ReadRecords.Load()
}

func (s *commonStats) GetBytesWritten() uint64 {
	return s.BytesWritten.Load()
}

func (s *commonStats) GetSIndexes() uint32 {
	return s.sIndexes.Load()
}

func (s *commonStats) GetUDFs() uint32 {
	return s.uDFs.Load()
}

func (s *commonStats) AddSIndexes(num uint32) {
	s.sIndexes.Add(num)
}

func (s *commonStats) AddUDFs(num uint32) {
	s.uDFs.Add(num)
}

func (s *commonStats) Start() {
	s.StartTime = time.Now()
}

func (s *commonStats) Stop() {
	if s.duration == 0 {
		s.duration = time.Since(s.StartTime)
	}
}

func (s *commonStats) GetDuration() time.Duration {
	return s.duration
}
