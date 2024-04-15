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
	"sync/atomic"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"github.com/google/uuid"
)

// **** Generic Restore Handler ****

// DBRestoreClient is an interface for writing data to a database
// The Aerospike Go client satisfies this interface
type DBRestoreClient interface {
	dbWriter
}

// worker is an interface for running a job
type worker interface {
	DoJob(context.Context, *pipeline.Pipeline[*models.Token]) error
}

// restoreHandlerBase handles generic restore jobs on data readers
// most other restore handlers can wrap this one to add additional functionality
type restoreHandlerBase struct {
	config   *RestoreConfig
	dbClient DBRestoreClient
	worker   worker
	stats    *RestoreStats
	logger   *slog.Logger
}

// RestoreHandler handles a restore job from a directory
type RestoreHandler struct {
	readerFactory   ReaderFactory
	config          *RestoreConfig
	aerospikeClient *a.Client
	logger          *slog.Logger
	errors          chan error
	id              string
	stats           RestoreStats
}

// newRestoreHandler creates a new RestoreHandler
func newRestoreHandler(config *RestoreConfig,
	ac *a.Client, logger *slog.Logger, readerFactory ReaderFactory) *RestoreHandler {
	id := uuid.NewString()
	logger = logging.WithHandler(logger, id, logging.HandlerTypeRestoreDirectory)

	return &RestoreHandler{
		config:          config,
		aerospikeClient: ac,
		id:              id,
		logger:          logger,
		readerFactory:   readerFactory,
	}
}

// run runs the restore job
// currently this should only be run once
func (rh *RestoreHandler) run(ctx context.Context) {
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
		var readersBuffer []io.Reader

		readers, err := rh.readerFactory.Readers()
		if err != nil {
			return err
		}

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
			if i < len(readers)-1 && len(readersBuffer) < rh.config.Parallel {
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
				rh.aerospikeClient, restoreWorker, &rh.stats, rh.logger)

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
func (rh *RestoreHandler) GetStats() *RestoreStats {
	return &rh.stats
}

// Wait waits for the restore job to complete and returns an error if the job failed
func (rh *RestoreHandler) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-rh.errors:
		return err
	}
}

// newRestoreHandlerBase creates a new restoreHandler
func newRestoreHandlerBase(config *RestoreConfig, ac DBRestoreClient,
	w worker, stats *RestoreStats, logger *slog.Logger) *restoreHandlerBase {
	logger.Debug("created new restore base handler")

	return &restoreHandlerBase{
		config:   config,
		dbClient: ac,
		worker:   w,
		stats:    stats,
		logger:   logger,
	}
}

// run runs the restore job
func (rh *restoreHandlerBase) run(ctx context.Context, readers []*readWorker[*models.Token]) error {
	rh.logger.Debug("running restore base handler")

	writeWorkers := make([]pipeline.Worker[*models.Token], rh.config.Parallel)

	for i := 0; i < rh.config.Parallel; i++ {
		var writer dataWriter[*models.Token] = newRestoreWriter(
			rh.dbClient,
			rh.config.WritePolicy,
			rh.logger,
		)

		writer = newWriterWithTokenStats(writer, rh.stats, rh.logger)
		writeWorkers[i] = newWriteWorker(writer)
	}

	processorWorkers := make([]pipeline.Worker[*models.Token], rh.config.Parallel)

	for i := 0; i < rh.config.Parallel; i++ {
		TTLSetter := newProcessorTTL(rh.stats, rh.logger)
		processorWorkers[i] = newProcessorWorker(TTLSetter)
	}

	readWorkers := make([]pipeline.Worker[*models.Token], len(readers))
	for i, r := range readers {
		readWorkers[i] = r
	}

	job := pipeline.NewPipeline(
		readWorkers,
		processorWorkers,
		writeWorkers,
	)

	return rh.worker.DoJob(ctx, job)
}

// **** Restore From Reader Handler ****

// RestoreStats stores the stats of a restore from reader job
type RestoreStats struct {
	tokenStats
	recordsExpired atomic.Uint64
}

func (rs *RestoreStats) GetRecordsExpired() uint64 {
	return rs.recordsExpired.Load()
}

func (rs *RestoreStats) addRecordsExpired(num uint64) {
	rs.recordsExpired.Add(num)
}
