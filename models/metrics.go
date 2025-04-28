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

// Metrics contains app metrics.
type Metrics struct {
	PipelineReadQueueSize  int
	PipelineWriteQueueSize int
	RecordsPerSecond       uint64
	KilobytesPerSecond     uint64
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

// Add returns a new Metrics object that is the sum of the receiver and another Metrics.
func (m *Metrics) Add(metrics ...*Metrics) *Metrics {
	if m == nil && len(metrics) == 0 {
		return nil
	}

	result := &Metrics{}
	if m != nil {
		// Create a copy of m's values, not a reference to m
		*result = *m
	}

	for _, other := range metrics {
		if other == nil {
			continue
		}

		result.PipelineReadQueueSize += other.PipelineReadQueueSize
		result.PipelineWriteQueueSize += other.PipelineWriteQueueSize
		result.RecordsPerSecond += other.RecordsPerSecond
		result.KilobytesPerSecond += other.KilobytesPerSecond
	}

	return result
}
