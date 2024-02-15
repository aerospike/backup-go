package backuplib

import (
	datahandlers "backuplib/data_handlers"
	"errors"
	"io"
)

// **** Generic Restore Handler ****

type RestoreOpts struct {
	Parallel int
}

type RestoreStatus struct {
	Active      bool
	RecordCount int
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

type restoreHandler struct {
	status   RestoreStatus
	args     RestoreOpts
	dbClient DBRestoreClient
	worker   worker
}

func newRestoreHandler(opts RestoreOpts, ac DBRestoreClient, w worker) *restoreHandler {
	return &restoreHandler{
		args:     opts,
		dbClient: ac,
		worker:   w,
	}
}

func (rh *restoreHandler) run(readers []datahandlers.Reader) error {

	processors := make([]datahandlers.Processor, rh.args.Parallel)
	for i := 0; i < rh.args.Parallel; i++ {
		processor := datahandlers.NewNOOPProcessor()
		processors[i] = processor
	}

	writers := make([]datahandlers.Writer, rh.args.Parallel)
	for i := 0; i < rh.args.Parallel; i++ {
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

func (*restoreHandler) GetStats() (RestoreStatus, error) {
	return RestoreStatus{}, errors.New("UNIMPLEMENTED")
}

// **** Restore From Reader Handler ****

type RestoreFromReaderOpts struct {
	RestoreOpts
}

type RestoreFromReaderStatus struct {
	RestoreStatus
}

type RestoreFromReaderHandler struct {
	status  RestoreFromReaderStatus
	args    RestoreFromReaderOpts
	dec     DecoderBuilder
	readers []io.Reader
	restoreHandler
}

func NewRestoreFromReaderHandler(opts RestoreFromReaderOpts, ac DBRestoreClient, dec DecoderBuilder, readers []io.Reader) *RestoreFromReaderHandler {
	worker := newWorkHandler()

	restoreHandler := newRestoreHandler(opts.RestoreOpts, ac, worker)

	return &RestoreFromReaderHandler{
		args:           opts,
		dec:            dec,
		readers:        readers,
		restoreHandler: *restoreHandler,
	}
}

// TODO don't expose this by moving it to the backuplib package
// we don't want users calling run directly
func (rrh *RestoreFromReaderHandler) run(readers []io.Reader) <-chan error {
	errors := make(chan error)

	go func(errChan chan<- error) {
		defer close(errChan)

		for _, reader := range readers {

			numDataReaders := rrh.args.Parallel
			dataReaders := make([]datahandlers.Reader, numDataReaders)

			for i := 0; i < numDataReaders; i++ {
				rrh.dec.SetSource(reader)
				decoder, err := rrh.dec.CreateDecoder()
				if err != nil {
					errChan <- err
					return
				}

				dataReaders[i] = datahandlers.NewGenericReader(decoder)
			}

			err := rrh.restoreHandler.run(dataReaders)
			if err != nil {
				errChan <- err
				return
			}
		}
	}(errors)

	return errors
}

func (rrh *RestoreFromReaderHandler) GetStats() (RestoreFromReaderStatus, error) {
	return RestoreFromReaderStatus{}, errors.New("UNIMPLEMENTED")
}
