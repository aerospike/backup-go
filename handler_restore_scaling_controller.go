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

	// Binary search state
	lowBound      int32
	highBound     int32
	bestBatchSize int32
	bestKBPS      uint64
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
		lowBound:         minBatchSizeThreshold,
		highBound:        maxBatchSizeThreshold,
		bestBatchSize:    initialBatchSize,
		bestKBPS:         0,
	}
}

func (c *restoreScalingController[T]) run(ctx context.Context, pl *pipe.Pipe[T]) {
	c.logger.Info("starting scaling controller")
	defer c.logger.Info("stopping scaling controller")

	ticker := time.NewTicker(scalingInterval)
	defer ticker.Stop()

	var lastAvgKBPS uint64
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

			avgKBPS := m.AverageKilobytesPerSecond

			// Decide what to do based on avgKBPS and queues
			action := c.decideScaling(m, avgKBPS, lastAvgKBPS, lastAction)

			switch action {
			case actionAddReader:
				c.logger.Info("dynamic scaling: adding reader",
					slog.Uint64("avg_kbps", avgKBPS),
					slog.Int("pr", m.PipelineReadQueueSize))
				reader := c.rh.readProcessor.newDataReader()
				if err := pl.AddReader(ctx, reader); err != nil {
					c.logger.Error("failed to add reader", slog.Any("error", err))
				}

			case actionAddWriter:
				c.logger.Info("dynamic scaling: adding writer",
					slog.Uint64("avg_kbps", avgKBPS),
					slog.Int("pw", m.PipelineWriteQueueSize))
				writer, err := c.rh.writeProcessor.newDataWriter(ctx, c.rh.useBatchWrites)
				if err != nil {
					c.logger.Error("failed to create writer", slog.Any("error", err))
				} else if err := pl.AddWriter(ctx, writer); err != nil {
					c.logger.Error("failed to add writer", slog.Any("error", err))
				}

			case actionAdjustBatchSize:
				currentBatchSize := c.dynamicBatchSize.Load()

				// Update bounds based on performance feedback.
				// Binary search logic: narrow the search space based on whether performance improved.
				if avgKBPS > c.bestKBPS {
					c.bestKBPS = avgKBPS
					c.bestBatchSize = currentBatchSize
					// Performance improved - values at or below current are known to work well,
					// so we can raise the lower bound to explore higher values.
					c.lowBound = currentBatchSize
				} else if avgKBPS < c.bestKBPS*95/100 { // 5% drop
					// Performance dropped significantly - current batch size is too high,
					// so we lower the upper bound.
					c.highBound = currentBatchSize
				}

				// Binary search next step
				var nextBatchSize int32
				if c.highBound-c.lowBound > 1 {
					nextBatchSize = (c.lowBound + c.highBound) / 2
					if nextBatchSize == currentBatchSize {
						// We are stuck, try to move
						if currentBatchSize < c.highBound {
							nextBatchSize++
						} else {
							nextBatchSize--
						}
					}
				} else {
					// We found the best? Or stuck.
					nextBatchSize = c.bestBatchSize
				}

				if nextBatchSize != currentBatchSize {
					c.logger.Info("dynamic scaling: adjusting batch size (binary search)",
						slog.Uint64("avg_kbps", avgKBPS),
						slog.Uint64("best_kbps", c.bestKBPS),
						slog.Int("old", int(currentBatchSize)),
						slog.Int("new", int(nextBatchSize)),
						slog.Int("low", int(c.lowBound)),
						slog.Int("high", int(c.highBound)))
					c.dynamicBatchSize.Store(nextBatchSize)
				} else {
					action = actionNone
				}

			case actionNone:
				// do nothing
			}

			lastAvgKBPS = avgKBPS
			lastAction = action
		}
	}
}

func (c *restoreScalingController[T]) decideScaling(
	m *models.Metrics,
	avgKBPS, lastAvgKBPS uint64,
	lastAction scalingAction,
) scalingAction {
	// 1. Check if previous action was successful
	if lastAction != actionNone {
		// If speed didn't grow significantly (at least 5%), consider it a failure or stagnation.
		// For workers, we stop adding. For batch size, we continue binary search.
		if avgKBPS <= lastAvgKBPS || (avgKBPS-lastAvgKBPS) < lastAvgKBPS/20 {
			if lastAction == actionAdjustBatchSize {
				return actionAdjustBatchSize
			}
			return actionNone
		}
	}

	avgQueueDepth := m.AverageQueueDepth

	// 2. Identify bottleneck based on average queue depth
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

	// 3. Queue depth is in acceptable range - use hill climbing to optimize further.
	// Look at individual queue balance to identify the bottleneck.

	// If read queue is significantly smaller, readers are the bottleneck.
	if m.PipelineReadQueueSize < m.PipelineWriteQueueSize/2 {
		return actionAddReader
	}
	// If write queue is significantly smaller, writers are the bottleneck.
	if m.PipelineWriteQueueSize < m.PipelineReadQueueSize/2 {
		return actionAddWriter
	}

	// Queues are balanced - try batch size optimization first.
	if lastAction != actionAdjustBatchSize {
		return actionAdjustBatchSize
	}

	// Fall back to adding workers based on which queue is smaller.
	if m.PipelineReadQueueSize < m.PipelineWriteQueueSize {
		return actionAddReader
	}
	return actionAddWriter
}
