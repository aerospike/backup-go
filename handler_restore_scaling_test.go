package backup

import (
	"testing"

	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/require"
)

func TestRestoreHandler_DecideScaling(t *testing.T) {
	t.Parallel()
	c := &restoreScalingController[*models.Token]{}

	t.Run("ActionAddReader when average queue depth is low", func(t *testing.T) {
		t.Parallel()
		m := &models.Metrics{
			PipelineReadQueueSize:  5,
			PipelineWriteQueueSize: 3,
			AverageQueueDepth:      5, // below lowQueueThreshold (10)
		}
		action := c.decideScaling(m, 1000, 1000, actionNone)
		require.Equal(t, actionAddReader, action)
	})

	t.Run("ActionAdjustBatchSize when average queue depth is high", func(t *testing.T) {
		t.Parallel()
		m := &models.Metrics{
			PipelineReadQueueSize:  100,
			PipelineWriteQueueSize: 150,
			AverageQueueDepth:      250, // above highQueueThreshold (200)
		}
		action := c.decideScaling(m, 1000, 1000, actionNone)
		require.Equal(t, actionAdjustBatchSize, action)
	})

	t.Run("ActionAddWriter when queue depth high and already tried batch size", func(t *testing.T) {
		t.Parallel()
		m := &models.Metrics{
			PipelineReadQueueSize:  100,
			PipelineWriteQueueSize: 150,
			AverageQueueDepth:      250, // above highQueueThreshold
		}
		action := c.decideScaling(m, 1100, 1000, actionAdjustBatchSize)
		require.Equal(t, actionAddWriter, action)
	})

	t.Run("ActionAdjustBatchSize (continue) even if speed didn't grow significantly", func(t *testing.T) {
		t.Parallel()
		m := &models.Metrics{
			PipelineReadQueueSize:  50,
			PipelineWriteQueueSize: 50,
			AverageQueueDepth:      100,
			KilobytesPerSecond:     1000,
		}
		// lastAvg was 1000, current avg is 1010 (only 1% growth)
		// Binary search should continue
		action := c.decideScaling(m, 1010, 1000, actionAdjustBatchSize)
		require.Equal(t, actionAdjustBatchSize, action)
	})

	t.Run("ActionNone when speed didn't grow after adding reader", func(t *testing.T) {
		t.Parallel()
		m := &models.Metrics{
			PipelineReadQueueSize:  25,
			PipelineWriteQueueSize: 25,
			AverageQueueDepth:      50,
		}
		action := c.decideScaling(m, 1000, 1000, actionAddReader)
		require.Equal(t, actionNone, action)
	})

	t.Run("Hill climbing when queues are balanced", func(t *testing.T) {
		t.Parallel()
		m := &models.Metrics{
			PipelineReadQueueSize:  50,
			PipelineWriteQueueSize: 60,
			AverageQueueDepth:      100, // within acceptable range
		}
		// Refined hill climbing favors batch size if last action was not batch size
		action := c.decideScaling(m, 1000, 1000, actionNone)
		require.Equal(t, actionAdjustBatchSize, action)

		// If last was batch size, it should pick based on queue size
		action = c.decideScaling(m, 1100, 1000, actionAdjustBatchSize)
		require.Equal(t, actionAddReader, action)

		m.PipelineReadQueueSize = 70
		action = c.decideScaling(m, 1100, 1000, actionAdjustBatchSize)
		require.Equal(t, actionAddWriter, action)
	})

	t.Run("Hill climbing when queues are significantly unbalanced", func(t *testing.T) {
		t.Parallel()
		m := &models.Metrics{
			PipelineReadQueueSize:  20,
			PipelineWriteQueueSize: 100,
			AverageQueueDepth:      100, // within acceptable range
		}
		// Read queue is significantly smaller, should add reader
		action := c.decideScaling(m, 1000, 1000, actionNone)
		require.Equal(t, actionAddReader, action)

		m.PipelineReadQueueSize = 100
		m.PipelineWriteQueueSize = 20
		// Write queue is significantly smaller, should add writer
		action = c.decideScaling(m, 1000, 1000, actionNone)
		require.Equal(t, actionAddWriter, action)
	})
}
