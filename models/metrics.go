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

import "sync"

// Metrics contains app metrics.
type Metrics struct {
	PipelineReadQueueSize  int
	PipelineWriteQueueSize int
	RecordsPerSecond       uint64
	KilobytesPerSecond     uint64

	// Average metrics (rolling window)
	AverageRecordsPerSecond   uint64
	AverageKilobytesPerSecond uint64
	AverageQueueDepth         uint64
}

// NewMetrics returns a new Metrics with the provided values.
func NewMetrics(
	pr, pw int,
	rps, kbps uint64,
) *Metrics {
	return &Metrics{
		PipelineReadQueueSize:  pr,
		PipelineWriteQueueSize: pw,
		RecordsPerSecond:       rps,
		KilobytesPerSecond:     kbps,
	}
}

// MovingAverage tracks moving average of values with thread-safe access.
type MovingAverage struct {
	mu     sync.Mutex
	values []uint64
	size   int
}

// NewMovingAverage creates a new MovingAverage with the given window size.
func NewMovingAverage(size int) *MovingAverage {
	return &MovingAverage{
		values: make([]uint64, 0, size),
		size:   size,
	}
}

// Add adds a value to the moving average.
func (ma *MovingAverage) Add(val uint64) {
	ma.mu.Lock()
	defer ma.mu.Unlock()

	if len(ma.values) >= ma.size {
		ma.values = ma.values[1:]
	}

	ma.values = append(ma.values, val)
}

// Average returns the average of the values.
func (ma *MovingAverage) Average() uint64 {
	ma.mu.Lock()
	defer ma.mu.Unlock()

	if len(ma.values) == 0 {
		return 0
	}

	var sum uint64
	for _, v := range ma.values {
		sum += v
	}

	return sum / uint64(len(ma.values))
}

// HistorySize returns the current number of values in history.
func (ma *MovingAverage) HistorySize() int {
	ma.mu.Lock()
	defer ma.mu.Unlock()

	return len(ma.values)
}

// SumMetrics returns a new Metrics object that is the sum of Metrics.
func SumMetrics(metrics ...*Metrics) *Metrics {
	if len(metrics) == 0 {
		return nil
	}

	result := &Metrics{}

	for _, one := range metrics {
		if one == nil {
			continue
		}

		result.PipelineReadQueueSize += one.PipelineReadQueueSize
		result.PipelineWriteQueueSize += one.PipelineWriteQueueSize
		result.RecordsPerSecond += one.RecordsPerSecond
		result.KilobytesPerSecond += one.KilobytesPerSecond
	}

	return result
}
