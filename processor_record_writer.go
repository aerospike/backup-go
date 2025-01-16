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
	"fmt"
	"log/slog"

	"github.com/aerospike/backup-go/internal/asinfo"
	"github.com/aerospike/backup-go/io/aerospike"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"golang.org/x/time/rate"
)

type recordWriterProcessor[T models.TokenConstraint] struct {
	aerospikeClient AerospikeClient
	config          *ConfigRestore
	stats           *models.RestoreStats
	limiter         *rate.Limiter

	logger *slog.Logger
}

func newRecordWriterProcessor[T models.TokenConstraint](
	aerospikeClient AerospikeClient,
	config *ConfigRestore,
	stats *models.RestoreStats,
	limiter *rate.Limiter,
	logger *slog.Logger,
) *recordWriterProcessor[T] {
	logger.Debug("created new records writer processor")

	return &recordWriterProcessor[T]{
		aerospikeClient: aerospikeClient,
		config:          config,
		stats:           stats,
		limiter:         limiter,
		logger:          logger,
	}
}

func (rw *recordWriterProcessor[T]) newWriterWorkers() ([]pipeline.Worker[T], error) {
	writeWorkers := make([]pipeline.Worker[T], rw.config.MaxAsyncBatches)

	useBatchWrites, err := rw.useBatchWrites()
	if err != nil {
		return nil, fmt.Errorf("failed to check batch writes: %w", err)
	}

	for i := 0; i < rw.config.MaxAsyncBatches; i++ {
		writer := aerospike.NewRestoreWriter[T](
			rw.aerospikeClient,
			rw.config.WritePolicy,
			rw.stats,
			rw.logger,
			useBatchWrites,
			rw.config.BatchSize,
			rw.config.RetryPolicy,
			rw.config.IgnoreRecordError,
		)

		statsWriter := newWriterWithTokenStats[T](writer, rw.stats, rw.logger)
		writeWorkers[i] = pipeline.NewWriteWorker[T](statsWriter, rw.limiter)
	}

	return writeWorkers, nil
}

func (rw *recordWriterProcessor[T]) useBatchWrites() (bool, error) {
	if rw.config.DisableBatchWrites {
		return false, nil
	}

	infoClient := asinfo.NewInfoClientFromAerospike(rw.aerospikeClient, rw.config.InfoPolicy)

	return infoClient.SupportsBatchWrite()
}
