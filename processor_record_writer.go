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
	"github.com/aerospike/backup-go/pkg/asinfo"
)

type recordWriterProcessor[T models.TokenConstraint] struct {
	aerospikeClient  AerospikeClient
	config           *ConfigRestore
	stats            *models.RestoreStats
	metricsCollector *metrics.Collector

	logger *slog.Logger
}

func newRecordWriterProcessor[T models.TokenConstraint](
	aerospikeClient AerospikeClient,
	config *ConfigRestore,
	stats *models.RestoreStats,
	metricsCollector *metrics.Collector,
	logger *slog.Logger,
) *recordWriterProcessor[T] {
	logger.Debug("created new records writer processor")

	return &recordWriterProcessor[T]{
		aerospikeClient:  aerospikeClient,
		config:           config,
		stats:            stats,
		metricsCollector: metricsCollector,
		logger:           logger,
	}
}

func (rw *recordWriterProcessor[T]) newDataWriters() ([]pipe.Writer[T], error) {
	var parallelism int

	switch {
	case rw.config.EncoderType == EncoderTypeASBX, rw.config.DisableBatchWrites:
		parallelism = rw.config.Parallel
	default:
		parallelism = rw.config.MaxAsyncBatches
	}

	// If we need only validation, we create discard writers.
	if rw.config.ValidateOnly {
		return newDiscardWriters[T](parallelism, rw.stats, rw.logger), nil
	}

	useBatchWrites, err := rw.useBatchWrites()
	if err != nil {
		return nil, fmt.Errorf("failed to check batch writes: %w", err)
	}

	dataWriters := make([]pipe.Writer[T], parallelism)

	for i := 0; i < parallelism; i++ {
		writer := aerospike.NewRestoreWriter[T](
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

		dataWriters[i] = newWriterWithTokenStats[T](writer, rw.stats, rw.logger)
	}

	return dataWriters, nil
}

func (rw *recordWriterProcessor[T]) useBatchWrites() (bool, error) {
	if rw.config.DisableBatchWrites {
		return false, nil
	}

	infoClient, err := asinfo.NewClient(
		rw.aerospikeClient.Cluster(),
		rw.config.InfoPolicy,
		rw.config.InfoRetryPolicy,
	)
	if err != nil {
		return false, fmt.Errorf("failed to create info client: %w", err)
	}

	return infoClient.SupportsBatchWrite(), nil
}

// discardWriter is a writer that does nothing. Used for backup files validation.
type discardWriter[T models.TokenConstraint] struct{}

// Write does nothing.
func (w *discardWriter[T]) Write(_ context.Context, _ T) (int, error) {
	return 0, nil
}

// Close does nothing.
func (w *discardWriter[T]) Close() error {
	return nil
}

// newDiscardWriters creates a slice of empty writers.
func newDiscardWriters[T models.TokenConstraint](
	parallelism int,
	stats statsSetterToken,
	logger *slog.Logger,
) []pipe.Writer[T] {
	dataWriters := make([]pipe.Writer[T], parallelism)
	for i := 0; i < parallelism; i++ {
		dataWriters[i] = newWriterWithTokenStats[T](&discardWriter[T]{}, stats, logger)
	}

	return dataWriters
}
