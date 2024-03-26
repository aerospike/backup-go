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
	"sync/atomic"

	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
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
}

// newRestoreHandlerBase creates a new restoreHandler
func newRestoreHandlerBase(config *RestoreConfig, ac DBRestoreClient,
	w worker, stats *RestoreStats) *restoreHandlerBase {
	return &restoreHandlerBase{
		config:   config,
		dbClient: ac,
		worker:   w,
		stats:    stats,
	}
}

// run runs the restore job
func (rh *restoreHandlerBase) run(ctx context.Context, readers []*readWorker[*models.Token]) error {
	writeWorkers := make([]pipeline.Worker[*models.Token], rh.config.Parallel)

	for i := 0; i < rh.config.Parallel; i++ {
		var writer dataWriter[*models.Token] = newRestoreWriter(
			rh.dbClient,
			rh.config.WritePolicy,
		)

		writer = newWriterWithTokenStats(writer, rh.stats)
		writeWorkers[i] = newWriteWorker(writer)
	}

	processorWorkers := make([]pipeline.Worker[*models.Token], rh.config.Parallel)

	for i := 0; i < rh.config.Parallel; i++ {
		TTLSetter := newProcessorTTL(rh.stats)
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

// RestoreHandler handles a restore job from a set of io.readers
type RestoreHandler struct {
	restoreHandlerBase
	config  *RestoreConfig
	errors  chan error
	readers []io.Reader
	stats   RestoreStats
}

// newRestoreHandler creates a new RestoreHandler
func newRestoreHandler(config *RestoreConfig, ac DBRestoreClient, readers []io.Reader) *RestoreHandler {
	rh := &RestoreHandler{
		config:  config,
		readers: readers,
	}

	worker := newWorkHandler()
	restoreHandler := newRestoreHandlerBase(config, ac, worker, &rh.stats)
	rh.restoreHandlerBase = *restoreHandler

	return rh
}

// run runs the restore job
// currently this should only be run once
func (rrh *RestoreHandler) run(ctx context.Context) {
	rrh.errors = make(chan error, 1)

	go func(errChan chan<- error) {
		// NOTE: order is important here
		// if we close the errChan before we handle the panic
		// the panic will attempt to send on a closed channel
		defer close(errChan)
		defer handlePanic(errChan)

		batchSize := rrh.config.Parallel
		dataReaders := []*readWorker[*models.Token]{}

		for i, reader := range rrh.readers {
			decoder, err := rrh.config.DecoderFactory.CreateDecoder(reader)
			if err != nil {
				errChan <- err
				return
			}

			dr := newTokenReader(decoder)
			readWorker := newReadWorker(dr)
			dataReaders = append(dataReaders, readWorker)
			// if we have not reached the batch size and we have more readers
			// continue to the next reader
			// if we are at the end of readers then run no matter what
			if i < len(rrh.readers)-1 && len(dataReaders) < batchSize {
				continue
			}

			err = rrh.restoreHandlerBase.run(ctx, dataReaders)
			if err != nil {
				errChan <- err
				return
			}

			clear(dataReaders)
		}
	}(rrh.errors)
}

// GetStats returns the stats of the restore job
func (rrh *RestoreHandler) GetStats() *RestoreStats {
	return &rrh.stats
}

// Wait waits for the restore job to complete and returns an error if the job failed
func (rrh *RestoreHandler) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-rrh.errors:
		return err
	}
}
