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
	"time"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/logic/logging"
	"github.com/aerospike/backup-go/logic/processors"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

// **** Generic Restore Handler ****

// DBRestoreClient is an interface for writing data to a database
// The Aerospike Go client satisfies this interface
type DBRestoreClient interface {
	dbWriter
}

// ReaderFactory provides access to data that should be restored.
type ReaderFactory interface {
	// Readers returns all available readers of backup data.
	// They will be used in parallel and closed after restore.
	Readers() ([]io.ReadCloser, error) //TODO: use lazy creation
	// GetType return type of storage. Used in logging.
	GetType() string
}

// RestoreHandler handles a restore job using the given readerFactory.
type RestoreHandler struct {
	readerFactory   ReaderFactory
	config          *RestoreConfig
	aerospikeClient *a.Client
	logger          *slog.Logger
	limiter         *rate.Limiter
	errors          chan error
	id              string
	stats           RestoreStats
}

// newRestoreHandler creates a new RestoreHandler
func newRestoreHandler(config *RestoreConfig,
	ac *a.Client, logger *slog.Logger, readerFactory ReaderFactory) *RestoreHandler {
	id := uuid.NewString()
	logger = logging.WithHandler(logger, id, logging.HandlerTypeRestore, readerFactory.GetType())

	return &RestoreHandler{
		config:          config,
		aerospikeClient: ac,
		id:              id,
		logger:          logger,
		readerFactory:   readerFactory,
		limiter:         makeBandwidthLimiter(config.Bandwidth),
	}
}

// run runs the restore job
// currently this should only be run once
func (rh *RestoreHandler) run(ctx context.Context) {
	rh.errors = make(chan error, 1)
	rh.stats.start = time.Now()

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

			readWorkers, err := rh.readersToReadWorkers(readersBuffer)
			if err != nil {
				return err
			}

			err = rh.runRestoreBatch(ctx, readWorkers)
			if err != nil {
				return err
			}

			readersBuffer = []io.Reader{}
		}

		return nil
	})
}

func (rh *RestoreHandler) readersToReadWorkers(readersBuffer []io.Reader) ([]*readWorker[*models.Token], error) {
	readWorkers := make([]*readWorker[*models.Token], len(readersBuffer))

	for i, reader := range readersBuffer {
		decoder, err := rh.config.DecoderFactory.CreateDecoder(reader)
		if err != nil {
			return nil, err
		}

		dr := newTokenReader(decoder, rh.logger)
		readWorker := newReadWorker[*models.Token](dr)
		readWorkers[i] = readWorker
	}

	return readWorkers, nil
}

// GetStats returns the stats of the restore job
func (rh *RestoreHandler) GetStats() *RestoreStats {
	return &rh.stats
}

// Wait waits for the restore job to complete and returns an error if the job failed
func (rh *RestoreHandler) Wait(ctx context.Context) error {
	defer func() {
		rh.stats.Duration = time.Since(rh.stats.start)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-rh.errors:
		return err
	}
}

// run runs the restore job
func (rh *RestoreHandler) runRestoreBatch(ctx context.Context, readers []*readWorker[*models.Token]) error {
	rh.logger.Debug("running restore base handler")

	writeWorkers := make([]pipeline.Worker[*models.Token], rh.config.Parallel)

	for i := 0; i < rh.config.Parallel; i++ {
		var writer dataWriter[*models.Token] = newRestoreWriter(
			rh.aerospikeClient,
			rh.config.WritePolicy,
			&rh.stats,
			rh.logger,
		)

		writer = newWriterWithTokenStats(writer, &rh.stats, rh.logger)
		writeWorkers[i] = newWriteWorker(writer, rh.limiter)
	}

	readWorkers := make([]pipeline.Worker[*models.Token], len(readers))
	for i, r := range readers {
		readWorkers[i] = r
	}

	recordCounter := newTokenWorker(processors.NewRecordCounter(&rh.stats.recordsTotal))
	sizeCounter := newTokenWorker(processors.NewSizeCounter(&rh.stats.totalBytesRead))
	changeNamespace := newTokenWorker(processors.NewChangeNamespace(rh.config.Namespace))
	ttlSetter := newTokenWorker(processors.NewExpirationSetter(&rh.stats.recordsExpired, rh.logger))
	binFilter := newTokenWorker(processors.NewFilterByBin(rh.config.BinList, &rh.stats.recordsSkipped))
	tpsLimiter := newTokenWorker(processors.NewTPSLimiter[*models.Token](ctx, rh.config.RecordsPerSecond))
	tokenTypeFilter := newTokenWorker(
		processors.NewFilterByType(rh.config.NoRecords, rh.config.NoIndexes, rh.config.NoUDFs))
	recordSetFilter := newTokenWorker(processors.NewFilterBySet(rh.config.SetList, &rh.stats.recordsSkipped))

	job := pipeline.NewPipeline(
		readWorkers,

		// in the pipeline, first all counters.
		recordCounter,
		sizeCounter,

		// filters
		tokenTypeFilter,
		recordSetFilter,
		binFilter,

		// speed limiters.
		tpsLimiter,

		// modifications.
		changeNamespace,
		ttlSetter,

		writeWorkers,
	)

	return job.Run(ctx)
}

func newTokenWorker(processor processors.TokenProcessor) []pipeline.Worker[*models.Token] {
	return []pipeline.Worker[*models.Token]{
		processors.NewProcessorWorker(processor),
	}
}

// **** Restore From Reader Handler ****

// RestoreStats stores the stats of a restore from reader job
type RestoreStats struct {
	start    time.Time
	Duration time.Duration
	tokenStats
	// The number of records dropped because they were expired.
	recordsExpired atomic.Uint64
	// The number of records dropped because they didn't contain any of the
	// selected bins or didn't belong to any of the selected sets.
	recordsSkipped atomic.Uint64
	// The number of records dropped because the database already contained the
	// records with a higher generation count.
	recordsFresher atomic.Uint64
	// The number of records dropped because they already existed in the
	// database.
	recordsExisted atomic.Uint64
	// The number of successfully restored records.
	recordsInserted atomic.Uint64
	// Total number of bytes read from source.
	totalBytesRead atomic.Uint64
}

func (rs *RestoreStats) GetRecordsExpired() uint64 {
	return rs.recordsExpired.Load()
}

func (rs *RestoreStats) GetRecordsSkipped() uint64 {
	return rs.recordsSkipped.Load()
}

func (rs *RestoreStats) GetRecordsFresher() uint64 {
	return rs.recordsFresher.Load()
}

func (rs *RestoreStats) incrRecordsFresher() {
	rs.recordsFresher.Add(1)
}

func (rs *RestoreStats) GetRecordsExisted() uint64 {
	return rs.recordsExisted.Load()
}

func (rs *RestoreStats) incrRecordsExisted() {
	rs.recordsExisted.Add(1)
}

func (rs *RestoreStats) GetRecordsInserted() uint64 {
	return rs.recordsInserted.Load()
}

func (rs *RestoreStats) incrRecordsInserted() {
	rs.recordsInserted.Add(1)
}

func (rs *RestoreStats) GetTotalBytesRead() uint64 {
	return rs.totalBytesRead.Load()
}
