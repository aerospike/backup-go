package backuplib

import (
	datahandlers "backuplib/data_handlers"
	"errors"
	"io"

	a "github.com/aerospike/aerospike-client-go/v7"
)

// **** Generic Restore Handler ****

type RestoreOpts struct {
	Parallel int
}

type RestoreStatus struct {
	Active      bool
	RecordCount int
}

type restoreHandler struct {
	status     RestoreStatus
	args       RestoreOpts
	aeroClient *a.Client
	worker     workHandler
}

func newRestoreHandler(opts RestoreOpts, ac *a.Client) *restoreHandler {
	wh := newWorkHandler()

	return &restoreHandler{
		args:       opts,
		aeroClient: ac,
		worker:     *wh,
	}
}

func (rh *restoreHandler) Run(readers []DataReader) error {

	processors := make([]DataProcessor, rh.args.Parallel)
	for i := 0; i < rh.args.Parallel; i++ {
		processor := datahandlers.NewNOOPProcessor()
		processors[i] = processor
	}

	writers := make([]DataWriter, rh.args.Parallel)
	for i := 0; i < rh.args.Parallel; i++ {
		writer := datahandlers.NewRestoreWriter(rh.aeroClient)
		writers[i] = writer
	}

	job := NewDataPipeline(
		readers,
		processors,
		writers,
	)

	return rh.worker.doJob(job)
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

func NewRestoreFromReaderHandler(opts RestoreFromReaderOpts, ac *a.Client, dec DecoderBuilder, readers []io.Reader) *RestoreFromReaderHandler {
	restoreHandler := newRestoreHandler(opts.RestoreOpts, ac)

	return &RestoreFromReaderHandler{
		args:           opts,
		dec:            dec,
		readers:        readers,
		restoreHandler: *restoreHandler,
	}
}

// TODO don't expose this by moving it to the backuplib package
// we don't want users calling Run directly
func (rrh *RestoreFromReaderHandler) Run(readers []io.Reader) <-chan error {
	errors := make(chan error)

	go func(errChan chan<- error) {
		defer close(errChan)

		for _, reader := range readers {

			numDataReaders := rrh.args.Parallel
			dataReaders := make([]DataReader, numDataReaders)

			for i := 0; i < numDataReaders; i++ {
				rrh.dec.SetSource(reader)
				decoder, err := rrh.dec.CreateDecoder()
				if err != nil {
					errChan <- err
					return
				}

				dataReaders[i] = datahandlers.NewGenericReader(decoder)
			}

			err := rrh.restoreHandler.Run(dataReaders)
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
