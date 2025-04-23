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

const (
	MetricRecordsPerSecond   = "rps"
	MetricKilobytesPerSecond = "kbps"
)

// Collector tracks and logs metrics such as request rate and counts within a context-managed environment.
type Collector struct {
	ctx     context.Context
	enabled bool
	name    string

	Increment func()
	Add       func(n uint64)

	counter  atomic.Uint64
	lastTime time.Time

	lastResultMu sync.RWMutex
	lastResult   float64

	logger *slog.Logger
}

// NewCollector initializes a new Collector with the provided context and logger,
// Enabled metrics will be reported to logger debug level.
func NewCollector(ctx context.Context, logger *slog.Logger, name string, enabled bool) *Collector {
	mc := &Collector{
		ctx:       ctx,
		enabled:   enabled,
		name:      name,
		Increment: func() {},
		Add:       func(uint64) {},
		lastTime:  time.Now(),
		logger:    logger,
	}

	logger.Debug("metrics", slog.String("name", name), slog.Bool("enabled", enabled))
	mc.Increment()

	if enabled {
		mc.Increment = func() { mc.counter.Add(1) }
		mc.Add = func(n uint64) { mc.counter.Add(n) }

		go mc.report()
	}

	return mc
}

// report periodically calculates and logs requests/records per second based on tracked request counts and elapsed time.
// It operates until the context of the Collector is canceled or its Done channel is closed.
func (mc *Collector) report() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case t := <-ticker.C:
			count := mc.counter.Swap(0)
			elapsed := t.Sub(mc.lastTime).Seconds()
			rps := float64(count) / elapsed

			// for kbps metric we should divide by 1024.
			if mc.name == MetricKilobytesPerSecond {
				rps /= 1024
			}

			mc.lastResultMu.Lock()
			mc.lastResult = rps
			mc.lastResultMu.Unlock()

			mc.logger.Debug("metrics, per second collector", slog.Float64(mc.name, rps))

			mc.lastTime = t
		case <-mc.ctx.Done():
			return
		}
	}
}

// GetLastResult returns the last calculated RecordsPerSecond value.
func (mc *Collector) GetLastResult() float64 {
	if mc == nil {
		return 0
	}

	mc.lastResultMu.RLock()
	defer mc.lastResultMu.RUnlock()

	return mc.lastResult
}
