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
	"fmt"
	"log/slog"

	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/io/aerospike"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe"
)

// recordWriterProcessor configures and creates record writers pipelines for restoring data.
type recordWriterProcessor struct {
	aerospikeClient  AerospikeClient
	config           *ConfigRestore
	stats            *models.RestoreStats
	metricsCollector *metrics.Collector
	infoClient       ClusterInfo

	logger *slog.Logger
}

// newRecordWriterProcessor returns a new record writer processor.
func newRecordWriterProcessor(
	aerospikeClient AerospikeClient,
	config *ConfigRestore,
	stats *models.RestoreStats,
	metricsCollector *metrics.Collector,
	infoClient ClusterInfo,
	logger *slog.Logger,
) *recordWriterProcessor {
	logger.Debug("created new records writer processor")

	return &recordWriterProcessor{
		aerospikeClient:  aerospikeClient,
		config:           config,
		stats:            stats,
		metricsCollector: metricsCollector,
		infoClient:       infoClient,
		logger:           logger,
	}
}

// newDataWriters creates the data writers for restoring data.
func (rw *recordWriterProcessor) newDataWriters(ctx context.Context) ([]pipe.Writer, error) {
	var parallelism int

	// Determine the parallelism based on the encoder type and batch writes support.
	switch {
	case rw.config.DisableBatchWrites:
		parallelism = rw.config.Parallel
	default:
		parallelism = rw.config.MaxAsyncBatches
	}

	// If we need only validation, we create discard writers.
	if rw.config.ValidateOnly {
		return newDiscardWriters(parallelism, rw.stats, rw.logger), nil
	}

	// Check if batch writes are supported.
	useBatchWrites, err := rw.useBatchWrites(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check batch writes: %w", err)
	}

	dataWriters := make([]pipe.Writer, parallelism)

	for i := 0; i < parallelism; i++ {
		writer := aerospike.NewRestoreWriter(
			ctx,
			rw.aerospikeClient,
			rw.config.WritePolicy,
			rw.stats,
			rw.logger,
			useBatchWrites,
			rw.config.BatchSize,
			rw.config.RetryPolicy,
			rw.metricsCollector,
			rw.config.IgnoreRecordError,
		)

		dataWriters[i] = newWriterWithTokenStats(writer, rw.stats, rw.logger)
	}

	return dataWriters, nil
}

// useBatchWrites checks if batch writes are supported.
func (rw *recordWriterProcessor) useBatchWrites(ctx context.Context) (bool, error) {
	if rw.config.DisableBatchWrites {
		return false, nil
	}

	return rw.infoClient.SupportsBatchWrite(ctx)
}

// discardWriter is a writer that does nothing. Used for backup files validation.
type discardWriter struct{}

// Write does nothing.
func (w *discardWriter) Write(_ *models.Token) (int, error) {
	return 0, nil
}

// Close does nothing.
func (w *discardWriter) Close() error {
	return nil
}

// newDiscardWriters creates a slice of empty writers.
func newDiscardWriters(
	parallelism int,
	stats statsSetterToken,
	logger *slog.Logger,
) []pipe.Writer {
	dataWriters := make([]pipe.Writer, parallelism)
	for i := range parallelism {
		dataWriters[i] = newWriterWithTokenStats(&discardWriter{}, stats, logger)
	}

	return dataWriters
}
