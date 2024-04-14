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
	"io"
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
)

// **** Restore From Directory Handler ****

// RestoreFromDirectoryStats stores the status of a restore from directory job
type RestoreFromDirectoryStats struct {
	RestoreStats
}

// RestoreFromDirectoryHandler handles a restore job from a directory
type RestoreFromDirectoryHandler struct {
	config          *RestoreConfig
	aerospikeClient *a.Client
	errors          chan error
	logger          *slog.Logger
	id              string
	stats           RestoreFromDirectoryStats
	readerFactory   ReaderFactory
}

// newRestoreFromDirectoryHandler creates a new RestoreFromDirectoryHandler
func newRestoreFromDirectoryHandler(config *RestoreConfig,
	ac *a.Client, logger *slog.Logger, readerFactory ReaderFactory) *RestoreFromDirectoryHandler {
	id := uuid.NewString()
	logger = logging.WithHandler(logger, id, logging.HandlerTypeRestoreDirectory)

	return &RestoreFromDirectoryHandler{
		config:          config,
		aerospikeClient: ac,
		id:              id,
		logger:          logger,
		readerFactory:   readerFactory,
	}
}

// run runs the restore job
// currently this should only be run once
func (rh *RestoreFromDirectoryHandler) run(ctx context.Context) {
	rh.errors = make(chan error, 1)

	go doWork(rh.errors, rh.logger, func() error {
		// check that the restore directory is valid
		// open the directory
		// read the files rrh.config.Parallel at a time
		// create a buffered reader for each reader
		// hand the readers to a restore handler and run it
		// wait for the restore handler to finish
		// if there are more files, continue to the next batch
		// if there are no more files, return

		readers, err := rh.readerFactory.Readers()
		if err != nil {
			return err
		}
		batchSize := rh.config.Parallel

		var readersBuffer []io.Reader
		for i, reader := range readers {
			//nolint:gocritic // defer in loop is ok here
			// we want to close the readers after the restore is done
			defer func() {
				if err := reader.Close(); err != nil {
					rh.logger.Error("failed to close backup reader", "error", err)
				}
			}()

			readersBuffer = append(readersBuffer, reader)

			// if we have not reached the batch size and we have more readers
			// continue to the next reader
			// if we are at the end of readers then run no matter what
			if i < len(readers)-1 && len(readersBuffer) < batchSize {
				continue
			}

			readWorkers := make([]*readWorker[*models.Token], len(readersBuffer))

			for i, reader := range readersBuffer {
				decoder, err := rh.config.DecoderFactory.CreateDecoder(reader)
				if err != nil {
					return err
				}

				dr := newTokenReader(decoder, rh.logger)
				readWorker := newReadWorker(dr)
				readWorkers[i] = readWorker
			}

			restoreWorker := newWorkHandler()
			restoreHandler := newRestoreHandlerBase(rh.config,
				rh.aerospikeClient, restoreWorker, &rh.stats.RestoreStats, rh.logger)

			err = restoreHandler.run(ctx, readWorkers)
			if err != nil {
				return err
			}

			readersBuffer = []io.Reader{}
		}

		return nil
	})
}

// GetStats returns the stats of the restore job
func (rh *RestoreFromDirectoryHandler) GetStats() *RestoreFromDirectoryStats {
	return &rh.stats
}

// Wait waits for the restore job to complete and returns an error if the job failed
func (rh *RestoreFromDirectoryHandler) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-rh.errors:
		return err
	}
}
