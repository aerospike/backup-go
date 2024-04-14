// Copyright 2024-2024 Aerospike, Inc.
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
	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
	"log/slog"
)

// **** Backup To Directory Handler ****

// BackupHandler handles a backup job to a directory
type BackupHandler struct {
	config          *BackupConfig
	aerospikeClient *a.Client
	errors          chan error
	logger          *slog.Logger
	id              string
	stats           BackupStats
	writeFactory    WriteFactory
}

// BackupStats stores the status of a backup job
// the stats are updated in realtime by backup jobs
type BackupStats struct {
	tokenStats
}

// newBackupGenericHandler creates a new BackupHandler
func newBackupGenericHandler(config *BackupConfig,
	ac *a.Client, logger *slog.Logger, writeFactory WriteFactory) *BackupHandler {
	id := uuid.NewString()
	logger = logging.WithHandler(logger, id, logging.HandlerTypeBackupDirectory)

	return &BackupHandler{
		config:          config,
		aerospikeClient: ac,
		id:              id,
		logger:          logger,
		writeFactory:    writeFactory,
	}
}

// run runs the backup job
// currently this should only be run once
func (bh *BackupHandler) run(ctx context.Context) {
	bh.errors = make(chan error, 1)
	go doWork(bh.errors, bh.logger, func() error {
		writeWorkers := make([]*writeWorker[*models.Token], bh.config.Parallel)
		for i := range bh.config.Parallel {
			encoder, err := bh.config.EncoderFactory.CreateEncoder()
			if err != nil {
				return err
			}

			writer, err := bh.writeFactory.NewWriter(bh.config.Namespace)
			if err != nil {
				return err
			}

			//nolint:gocritic // defer in loop is ok here,
			// we want to close the file after the backup is done
			defer func() {
				if err := writer.Close(); err != nil {
					bh.logger.Error("failed to close backup file", "error", err)
				}
			}()

			var dataWriter dataWriter[*models.Token] = newTokenWriter(encoder, writer, bh.logger)
			dataWriter = newWriterWithTokenStats(dataWriter, &bh.stats, bh.logger)
			writeWorkers[i] = newWriteWorker(dataWriter)
		}

		handler := newBackupHandlerBase(bh.config, bh.aerospikeClient, bh.logger)

		return handler.run(ctx, writeWorkers)
	})
}

// GetStats returns the stats of the backup job
func (bh *BackupHandler) GetStats() *BackupStats {
	return &bh.stats
}

// Wait waits for the backup job to complete and returns an error if the job failed
func (bh *BackupHandler) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-bh.errors:
		return err
	}
}
