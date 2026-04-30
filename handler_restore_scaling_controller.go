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

package backup

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe"
)

const (
	scalingInterval = 5 * time.Second
	// Thresholds for average queue depth (read + write queue lengths)
	lowQueueThreshold  = 10
	highQueueThreshold = 1000

	maxBatchSizeThreshold = 1024
	minBatchSizeThreshold = 1
)

type scalingAction int

const (
	actionNone scalingAction = iota
	actionAddReader
	actionAddWriter
	actionAdjustBatchSize
)

type restoreScalingController[T models.TokenConstraint] struct {
	rh               *RestoreHandler[T]
	logger           *slog.Logger
	dynamicBatchSize *atomic.Int32

	// Smoothed metrics
	emaKBPS float64

	// Hill climbing state for batch size
	bestBatchSize int32
	bestKBPS      float64
	batchStep     int32
	direction     int32 // 1 for increasing, -1 for decreasing

	cooldownTicks int
}

func newRestoreScalingController[T models.TokenConstraint](
	rh *RestoreHandler[T],
	dynamicBatchSize *atomic.Int32,
) *restoreScalingController[T] {
	initialBatchSize := dynamicBatchSize.Load()
	return &restoreScalingController[T]{
		rh:               rh,
		logger:           rh.logger,
		dynamicBatchSize: dynamicBatchSize,
		bestBatchSize:    initialBatchSize,
		bestKBPS:         0,
		batchStep:        32, // Start with a reasonable step size
		direction:        1,  // Start by increasing
	}
}

func (c *restoreScalingController[T]) run(ctx context.Context, pl *pipe.Pipe[T]) {
	c.logger.Info("starting scaling controller")
	defer c.logger.Info("stopping scaling controller")

	ticker := time.NewTicker(scalingInterval)
	defer ticker.Stop()

	var lastAction scalingAction

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m := c.rh.GetMetrics()
			if m == nil {
				continue
			}

			// Update Exponential Moving Average (EMA) of KBPS to smooth out noise (alpha = 0.3)
			currentKBPS := float64(m.AverageKilobytesPerSecond)
			if c.emaKBPS == 0 {
				c.emaKBPS = currentKBPS
			} else {
				c.emaKBPS = 0.3*currentKBPS + 0.7*c.emaKBPS
			}

			if c.cooldownTicks > 0 {
				c.cooldownTicks--
				continue
			}

			// Decide what to do based on emaKBPS and queues
			action := c.decideScaling(m, c.emaKBPS, lastAction)

			switch action {
			case actionAddReader:
				c.logger.Info("dynamic scaling: adding reader",
					slog.Float64("ema_kbps", c.emaKBPS),
					slog.Int("pr", m.PipelineReadQueueSize))
				reader := c.rh.readProcessor.newDataReader()
				if err := pl.AddReader(ctx, reader); err != nil {
					c.logger.Error("failed to add reader", slog.Any("error", err))
				}
				c.cooldownTicks = 1 // Wait 1 tick for the new worker to ramp up

			case actionAddWriter:
				c.logger.Info("dynamic scaling: adding writer",
					slog.Float64("ema_kbps", c.emaKBPS),
					slog.Int("pw", m.PipelineWriteQueueSize))
				writer, err := c.rh.writeProcessor.newDataWriter(ctx, c.rh.useBatchWrites)
				if err != nil {
					c.logger.Error("failed to create writer", slog.Any("error", err))
				} else if err := pl.AddWriter(ctx, writer); err != nil {
					c.logger.Error("failed to add writer", slog.Any("error", err))
				}
				c.cooldownTicks = 1

			case actionAdjustBatchSize:
				currentBatchSize := c.dynamicBatchSize.Load()

				// Hill Climbing optimization
				if c.emaKBPS > c.bestKBPS {
					// Improved! Keep going in the same direction.
					c.bestKBPS = c.emaKBPS
					c.bestBatchSize = currentBatchSize
					// Optionally increase step size if we are making good progress
				} else {
					// Performance dropped. Reverse direction and reduce step size to fine-tune.
					c.direction *= -1
					if c.batchStep > 1 {
						c.batchStep /= 2
					}
				}

				nextBatchSize := currentBatchSize + (c.batchStep * c.direction)

				// Clamp to bounds
				if nextBatchSize > maxBatchSizeThreshold {
					nextBatchSize = maxBatchSizeThreshold
					c.direction = -1
				} else if nextBatchSize < minBatchSizeThreshold {
					nextBatchSize = minBatchSizeThreshold
					c.direction = 1
				}

				if nextBatchSize != currentBatchSize {
					c.logger.Info("dynamic scaling: adjusting batch size (hill climbing)",
						slog.Float64("ema_kbps", c.emaKBPS),
						slog.Float64("best_kbps", c.bestKBPS),
						slog.Int("old", int(currentBatchSize)),
						slog.Int("new", int(nextBatchSize)),
						slog.Int("step", int(c.batchStep)),
						slog.Int("dir", int(c.direction)))
					c.dynamicBatchSize.Store(nextBatchSize)
					c.cooldownTicks = 1 // Wait 1 tick to see the effect
				} else {
					action = actionNone
				}

			case actionNone:
				// do nothing
			}

			if action != actionNone {
				lastAction = action
			}
		}
	}
}

func (c *restoreScalingController[T]) decideScaling(
	m *models.Metrics,
	emaKBPS float64,
	lastAction scalingAction,
) scalingAction {
	avgQueueDepth := m.AverageQueueDepth

	// 1. Identify bottleneck based on average queue depth
	// Low queue depth -> pipeline is starved, need more readers
	if avgQueueDepth < lowQueueThreshold {
		return actionAddReader
	}

	// High queue depth -> pipeline is backed up, need more consumers
	if avgQueueDepth > highQueueThreshold {
		// Try adjusting batch size first as it's cheaper than adding a new writer.
		if lastAction != actionAdjustBatchSize {
			return actionAdjustBatchSize
		}
		return actionAddWriter
	}

	// 2. Queue depth is in acceptable range - use individual queue balance to identify the bottleneck.
	// If read queue is significantly smaller, readers are the bottleneck.
	if m.PipelineReadQueueSize < m.PipelineWriteQueueSize/2 {
		return actionAddReader
	}
	// If write queue is significantly smaller, writers are the bottleneck.
	if m.PipelineWriteQueueSize < m.PipelineReadQueueSize/2 {
		return actionAddWriter
	}

	// 3. Queues are balanced - prioritize batch size optimization (hill climbing).
	// Batch size can always be tweaked for potential performance gains without adding new workers.
	return actionAdjustBatchSize
}
