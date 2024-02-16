package backuplib

import (
	datahandlers "backuplib/data_handlers"
	"errors"
	"io"
)

// **** Generic Restore Handler ****

type RestoreStatus struct {
	Active bool
}

// DBRestoreClient is an interface for writing data to a database
// The Aerospike Go client satisfies this interface
type DBRestoreClient interface {
	datahandlers.DBWriter
}

// worker is an interface for running a job
type worker interface {
	DoJob(*datahandlers.DataPipeline) error
}

// restoreHandler handles generic restore jobs on data readers
// most other restore handlers can wrap this one to add additional functionality
type restoreHandler struct {
	status   *RestoreStatus
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
func (rh *restoreHandler) run(readers []datahandlers.Reader) error {

	processors := make([]datahandlers.Processor, rh.config.Parallel)
	for i := 0; i < rh.config.Parallel; i++ {
		processor := datahandlers.NewNOOPProcessor()
		processors[i] = processor
	}

	writers := make([]datahandlers.Writer, rh.config.Parallel)
	for i := 0; i < rh.config.Parallel; i++ {
		writer := datahandlers.NewRestoreWriter(rh.dbClient)
		writers[i] = writer
	}

	job := datahandlers.NewDataPipeline(
		readers,
		processors,
		writers,
	)

	return rh.worker.DoJob(job)
}

// GetStats returns the status of the restore job
func (*restoreHandler) GetStats() (RestoreStatus, error) {
	return RestoreStatus{}, errors.New("UNIMPLEMENTED")
}

// **** Restore From Reader Handler ****

// RestoreFromReaderStatus stores the status of a restore from reader job
type RestoreFromReaderStatus struct {
	RestoreStatus
}

// RestoreFromReaderHandler handles restore jobs from a set of io.readers
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
		defer close(errChan)

		batchSize := rrh.config.Parallel
		dataReaders := []datahandlers.Reader{}

		for i, reader := range readers {

			rrh.config.DecoderBuilder.SetSource(reader)
			decoder, err := rrh.config.DecoderBuilder.CreateDecoder()
			if err != nil {
				errChan <- err
				return
			}

			dr := datahandlers.NewGenericReader(decoder)
			dataReaders = append(dataReaders, dr)
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
func (rrh *RestoreFromReaderHandler) GetStats() (RestoreFromReaderStatus, error) {
	return RestoreFromReaderStatus{}, errors.New("UNIMPLEMENTED")
}

// Wait waits for the restore job to complete and returns an error if the job failed
func (rrh *RestoreFromReaderHandler) Wait() error {
	return <-rrh.errors
}
