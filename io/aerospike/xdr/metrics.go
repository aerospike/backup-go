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
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

type metricsCollector struct {
	ctx context.Context

	requestCount atomic.Uint64
	increment    func()
	lastTime     time.Time

	logger *slog.Logger
}

func mewMetricsCollector(ctx context.Context, logger *slog.Logger) *metricsCollector {
	mc := &metricsCollector{
		ctx:       ctx,
		increment: func() {},
		lastTime:  time.Now(),
		logger:    logger,
	}

	// enable only for logger debug level
	if mc.logger.Enabled(mc.ctx, slog.LevelDebug) {
		mc.increment = func() { mc.requestCount.Add(1) }
		go mc.reportMetrics()
	}

	return mc
}

func (mc *metricsCollector) reportMetrics() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case t := <-ticker.C:
			count := mc.requestCount.Swap(0)
			elapsed := t.Sub(mc.lastTime).Seconds()
			rps := float64(count) / elapsed

			mc.logger.Debug("metrics", slog.Float64("rps", rps))

			mc.lastTime = t
		case <-mc.ctx.Done():
			return
		}
	}
}
