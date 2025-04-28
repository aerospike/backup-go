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
	"sync/atomic"
	"time"
)

const (
	MetricRecordsPerSecond   = "rps"
	MetricKilobytesPerSecond = "kbps"
)

// Collector tracks and logs metrics such as request rate and counts within a context-managed environment.
type Collector struct {
	ctx context.Context

	// enabled indicates whether the Collector is active and metrics will be tracked and reported.
	enabled bool
	// name of the metric, is used for logging.
	name string
	// message is used for logging.
	message string
	// Increment and Add are used to track and report metrics.
	Increment func()
	Add       func(n uint64)

	// processed tracks the total number of processed requests or operations using an atomic counter.
	processed atomic.Uint64
	// lastTime tracks the last time the metrics were reported.
	lastTime time.Time
	// lastResult tracks the last calculated RecordsPerSecond value.
	lastResult atomic.Uint64
	// logger is used for logging.
	logger *slog.Logger
}

// NewCollector initializes a new Collector with the provided context and logger,
// Enabled metrics will be reported to logger debug level.
func NewCollector(ctx context.Context, logger *slog.Logger, name, message string, enabled bool) *Collector {
	mc := &Collector{
		ctx:       ctx,
		enabled:   enabled,
		name:      name,
		message:   message,
		Increment: func() {},
		Add:       func(uint64) {},
		lastTime:  time.Now(),
		logger:    logger,
	}

	logger.Debug(
		"metrics",
		slog.String("name", name),
		slog.String("message", message),
		slog.Bool("enabled", enabled),
	)
	mc.Increment()

	if enabled {
		mc.Increment = func() { mc.processed.Add(1) }
		mc.Add = func(n uint64) { mc.processed.Add(n) }

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
			count := mc.processed.Swap(0)
			elapsed := t.Sub(mc.lastTime).Seconds()
			result := float64(count) / elapsed

			// for kbps metric we should divide by 1024.
			if mc.name == MetricKilobytesPerSecond {
				result /= 1024
			}

			mc.lastResult.Store(uint64(result))
			mc.logger.Debug(mc.message, slog.Float64(mc.name, result))
			mc.lastTime = t
		case <-mc.ctx.Done():
			return
		}
	}
}

// GetLastResult returns the last calculated RecordsPerSecond value.
func (mc *Collector) GetLastResult() uint64 {
	if mc == nil {
		return 0
	}

	return mc.lastResult.Load()
}
