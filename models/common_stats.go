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
	if s.duration == 0 {
		return time.Since(s.StartTime)
	}

	return s.duration
}

// sumCommonStats combines multiple commonStats.
func sumCommonStats(stats ...*commonStats) *commonStats {
	result := &commonStats{}

	for _, stat := range stats {
		if stat == nil {
			continue
		}

		if result.StartTime.IsZero() || stat.StartTime.Before(result.StartTime) {
			result.StartTime = stat.StartTime
		}

		result.ReadRecords.Add(stat.GetReadRecords())
		result.sIndexes.Add(stat.GetSIndexes())
		result.uDFs.Add(stat.GetUDFs())
		result.BytesWritten.Add(stat.GetBytesWritten())
	}

	return result
}
