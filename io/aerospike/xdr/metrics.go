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

package xdr

import (
	"log/slog"
	"sync/atomic"
	"time"
)

type MetricsCollector struct {
	requestCount uint64
	lastTime     time.Time
	logger       *slog.Logger
}

func NewMetricsCollector(logger *slog.Logger) *MetricsCollector {
	mc := &MetricsCollector{
		lastTime: time.Now(),
		logger:   logger,
	}
	go mc.reportMetrics()

	return mc
}

func (mc *MetricsCollector) IncrementRequests() {
	atomic.AddUint64(&mc.requestCount, 1)
}

func (mc *MetricsCollector) reportMetrics() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		count := atomic.SwapUint64(&mc.requestCount, 0)
		elapsed := now.Sub(mc.lastTime).Seconds()
		rps := float64(count) / elapsed

		mc.logger.Debug("metrics", slog.Float64("rps", rps))

		mc.lastTime = now
	}
}
