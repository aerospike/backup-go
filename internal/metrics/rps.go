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

package metrics

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// RPSCollector tracks and logs metrics such as request rate and counts within a context-managed environment.
type RPSCollector struct {
	ctx context.Context

	requestCount atomic.Uint64
	Increment    func()
	lastTime     time.Time

	resultMu   sync.RWMutex
	lastResult float64

	logger *slog.Logger
}

// NewRPSCollector initializes a new RPSCollector with the provided context and logger, enabling debug-level metrics.
func NewRPSCollector(ctx context.Context, logger *slog.Logger) *RPSCollector {
	mc := &RPSCollector{
		ctx:       ctx,
		Increment: func() {},
		lastTime:  time.Now(),
		logger:    logger,
	}

	// enable only for logger debug level
	if mc.logger.Enabled(mc.ctx, slog.LevelDebug) {
		mc.Increment = func() { mc.requestCount.Add(1) }
		go mc.report()
	}

	return mc
}

// report periodically calculates and logs requests/records per second based on tracked request counts and elapsed time.
// It operates until the context of the RPSCollector is canceled or its Done channel is closed.
func (mc *RPSCollector) report() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case t := <-ticker.C:
			count := mc.requestCount.Swap(0)
			elapsed := t.Sub(mc.lastTime).Seconds()
			rps := float64(count) / elapsed
			mc.resultMu.Lock()
			mc.lastResult = rps
			mc.resultMu.Unlock()

			mc.logger.Debug("metrics", slog.Float64("rps", rps))

			mc.lastTime = t
		case <-mc.ctx.Done():
			return
		}
	}
}

// GetLastResult returns the last calculated RecordsPerSecond value.
func (mc *RPSCollector) GetLastResult() float64 {
	if mc == nil {
		return 0
	}

	mc.resultMu.RLock()
	defer mc.resultMu.RUnlock()

	return mc.lastResult
}
