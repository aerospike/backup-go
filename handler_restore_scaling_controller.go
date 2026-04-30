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
	scalingInterval = 1 * time.Second
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

	// Hill climbing state for batch size
	bestBatchSize int32
	bestKBPS      float64
	batchStep     int32
	direction     int32 // 1 for increasing, -1 for decreasing

	kbpsWindow         []float64
	windowSize         int
	batchSizeOptimized bool
	stopAddingWorkers  bool
	lastActionKBPS     float64
}

func newRestoreScalingController[T models.TokenConstraint](
	rh *RestoreHandler[T],
	dynamicBatchSize *atomic.Int32,
) *restoreScalingController[T] {
	initialBatchSize := dynamicBatchSize.Load()
	return &restoreScalingController[T]{
		rh:                 rh,
		logger:             rh.logger,
		dynamicBatchSize:   dynamicBatchSize,
		bestBatchSize:      initialBatchSize,
		bestKBPS:           0,
		batchStep:          64,                    // Start with a larger step size to find peak faster
		direction:          1,                     // Start by increasing
		kbpsWindow:         make([]float64, 0, 5), // 5 ticks = 5 seconds
		windowSize:         5,
		batchSizeOptimized: false,
		stopAddingWorkers:  false,
		lastActionKBPS:     0,
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

			// Initialize kbpsWindow array here if it's the first time
			if len(c.kbpsWindow) == 0 {
				c.kbpsWindow = make([]float64, 0, c.windowSize)
			}

			currentKBPS := float64(m.AverageKilobytesPerSecond)
			c.kbpsWindow = append(c.kbpsWindow, currentKBPS)

			// Wait until we have enough data to form a window
			if len(c.kbpsWindow) < c.windowSize {
				continue
			}

			// Calculate average of the non-overlapping window
			var sum float64
			for _, v := range c.kbpsWindow {
				sum += v
			}
			windowKBPS := sum / float64(c.windowSize)

			// Clear the window to start collecting a fresh one for the next action evaluation
			c.kbpsWindow = c.kbpsWindow[:0]

			// Decide what to do based on the stable windowKBPS
			action := c.decideScaling(m, windowKBPS, lastAction)

		executeAction:
			switch action {
			case actionAddReader:
				c.logger.Info("dynamic scaling: adding reader",
					slog.Float64("window_kbps", windowKBPS),
					slog.Int("pr", m.PipelineReadQueueSize))
				reader := c.rh.readProcessor.newDataReader()
				if err := pl.AddReader(ctx, reader); err != nil {
					c.logger.Error("failed to add reader", slog.Any("error", err))
				}
				c.lastActionKBPS = windowKBPS

			case actionAddWriter:
				c.logger.Info("dynamic scaling: adding writer",
					slog.Float64("window_kbps", windowKBPS),
					slog.Int("pw", m.PipelineWriteQueueSize))
				writer, err := c.rh.writeProcessor.newDataWriter(ctx, c.rh.useBatchWrites)
				if err != nil {
					c.logger.Error("failed to create writer", slog.Any("error", err))
				} else if err := pl.AddWriter(ctx, writer); err != nil {
					c.logger.Error("failed to add writer", slog.Any("error", err))
				}
				c.lastActionKBPS = windowKBPS

			case actionAdjustBatchSize:
				currentBatchSize := c.dynamicBatchSize.Load()

				if c.bestKBPS == 0 {
					// First evaluation
					c.bestKBPS = windowKBPS
					c.bestBatchSize = currentBatchSize
				} else if windowKBPS > c.bestKBPS*1.01 {
					// Improved by at least 1%! Keep going in the same direction.
					c.bestKBPS = windowKBPS
					c.bestBatchSize = currentBatchSize
				} else {
					// Performance dropped or didn't improve by 1%.
					// Reverse direction and reduce step size to fine-tune.
					c.direction *= -1
					if c.batchStep > 2 {
						c.batchStep /= 2
					} else {
						// Step size is minimal, we found the local peak!
						c.batchSizeOptimized = true

						// Revert to the best known batch size we found during the climb
						if currentBatchSize != c.bestBatchSize {
							c.dynamicBatchSize.Store(c.bestBatchSize)
						}

						c.logger.Info("dynamic scaling: batch size optimized, shifting to worker scaling",
							slog.Int("best_batch_size", int(c.bestBatchSize)),
							slog.Float64("best_kbps", c.bestKBPS))

						// Force immediate evaluation of worker scaling on this exact tick
						action = c.decideScaling(m, windowKBPS, lastAction)
						// Ensure we process the new action immediately instead of skipping
						goto executeAction
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
						slog.Float64("window_kbps", windowKBPS),
						slog.Float64("best_kbps", c.bestKBPS),
						slog.Int("old", int(currentBatchSize)),
						slog.Int("new", int(nextBatchSize)),
						slog.Int("step", int(c.batchStep)),
						slog.Int("dir", int(c.direction)))
					c.dynamicBatchSize.Store(nextBatchSize)
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
	windowKBPS float64,
	lastAction scalingAction,
) scalingAction {
	// Phase 1: Exclusively focus on finding the best possible batch size.
	// Ignore all queue depths and worker metrics until this is fully tuned.
	if !c.batchSizeOptimized {
		return actionAdjustBatchSize
	}

	// Phase 2: Batch size is optimized. Now manage workers if needed.

	// Check if previous worker addition was successful
	if lastAction == actionAddReader || lastAction == actionAddWriter {
		// We expect at least a 2% increase in windowKBPS after adding a worker.
		if windowKBPS <= c.lastActionKBPS*1.02 {
			c.logger.Info("dynamic scaling: worker addition did not improve throughput significantly, pausing worker scaling")
			c.stopAddingWorkers = true
		}
	}

	if c.stopAddingWorkers {
		return actionNone
	}

	avgQueueDepth := m.AverageQueueDepth

	// Identify bottleneck based on average queue depth
	if avgQueueDepth < lowQueueThreshold {
		return actionAddReader
	}

	if avgQueueDepth > highQueueThreshold {
		return actionAddWriter
	}

	// Queue depth is in acceptable range - use individual queue balance to identify the bottleneck.
	if m.PipelineReadQueueSize < m.PipelineWriteQueueSize/2 {
		return actionAddReader
	}
	if m.PipelineWriteQueueSize < m.PipelineReadQueueSize/2 {
		return actionAddWriter
	}

	return actionNone
}
