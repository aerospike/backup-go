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

package backuplib

import (
	"io"

	"github.com/aerospike/aerospike-tools-backup-lib/models"
	"github.com/aerospike/aerospike-tools-backup-lib/pipeline"
)

// **** Generic Restore Handler ****

// RestoreStatus stores the status of a restore job
// TODO fill this out
type RestoreStatus struct{}

// DBRestoreClient is an interface for writing data to a database
// The Aerospike Go client satisfies this interface
type DBRestoreClient interface {
	DBWriter
}

// worker is an interface for running a job
type worker interface {
	// TODO change the any typed pipeline to a message or token type
	DoJob(*pipeline.Pipeline[*models.Token]) error
}

// restoreHandler handles generic restore jobs on data readers
// most other restore handlers can wrap this one to add additional functionality
type restoreHandler struct {
	config   *RestoreBaseConfig
	dbClient DBRestoreClient
	worker   worker
}

// newRestoreHandler creates a new restoreHandler
func newRestoreHandler(config *RestoreBaseConfig, ac DBRestoreClient, w worker) *restoreHandler {
	return &restoreHandler{
		config:   config,
		dbClient: ac,
		worker:   w,
	}
}

// run runs the restore job
func (rh *restoreHandler) run(readers []*ReadWorker[*models.Token]) error {

	processorWorkers := make([]pipeline.Worker[*models.Token], rh.config.Parallel)
	for i := 0; i < rh.config.Parallel; i++ {
		processor := NewNoOpProcessor()
		processorWorkers[i] = NewProcessorWorker(processor)
	}

	writeWorkers := make([]pipeline.Worker[*models.Token], rh.config.Parallel)
	for i := 0; i < rh.config.Parallel; i++ {
		writer := NewRestoreWriter(rh.dbClient)
		writeWorkers[i] = NewWriteWorker(writer)
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

	return rh.worker.DoJob(job)
}

// **** Restore From Reader Handler ****

// RestoreFromReaderStatus stores the status of a restore from reader job
type RestoreFromReaderStatus struct {
	RestoreStatus
}

// RestoreFromReaderHandler handles a restore job from a set of io.readers
type RestoreFromReaderHandler struct {
	status  *RestoreFromReaderStatus
	config  *RestoreFromReaderConfig
	readers []io.Reader
	errors  chan error
	restoreHandler
}

// newRestoreFromReaderHandler creates a new RestoreFromReaderHandler
func newRestoreFromReaderHandler(config *RestoreFromReaderConfig, ac DBRestoreClient, readers []io.Reader) *RestoreFromReaderHandler {
	worker := newWorkHandler()

	restoreHandler := newRestoreHandler(&config.RestoreBaseConfig, ac, worker)

	return &RestoreFromReaderHandler{
		config:         config,
		readers:        readers,
		restoreHandler: *restoreHandler,
	}
}

// run runs the restore job
// currently this should only be run once
func (rrh *RestoreFromReaderHandler) run(readers []io.Reader) {
	rrh.errors = make(chan error)

	go func(errChan chan<- error) {

		// NOTE: order is important here
		// if we close the errChan before we handle the panic
		// the panic will attempt to send on a closed channel
		defer close(errChan)
		defer handlePanic(errChan)

		batchSize := rrh.config.Parallel
		// TODO change the any type to a message or token type
		dataReaders := []*ReadWorker[*models.Token]{}

		for i, reader := range readers {

			rrh.config.DecoderBuilder.SetSource(reader)
			decoder, err := rrh.config.DecoderBuilder.CreateDecoder()
			if err != nil {
				errChan <- err
				return
			}

			dr := NewGenericReader(decoder)
			readWorker := NewReadWorker(dr)
			dataReaders = append(dataReaders, readWorker)
			// if we have not reached the batch size and we have more readers
			// continue to the next reader
			// if we are at the end of readers then run no matter what
			if i < len(readers)-1 && len(dataReaders) < batchSize {
				continue
			}

			err = rrh.restoreHandler.run(dataReaders)
			if err != nil {
				errChan <- err
				return
			}

			clear(dataReaders)
		}
	}(rrh.errors)
}

// GetStats returns the stats of the restore job
func (rrh *RestoreFromReaderHandler) GetStats() RestoreFromReaderStatus {
	return *rrh.status
}

// Wait waits for the restore job to complete and returns an error if the job failed
func (rrh *RestoreFromReaderHandler) Wait() error {
	return <-rrh.errors
}
