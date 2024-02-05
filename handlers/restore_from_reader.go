package handlers

import (
	datahandlers "backuplib/data_handlers"
	"errors"
	"io"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type Decoder interface {
	NextToken() (any, error)
}

type DecoderFactory interface {
	CreateDecoder(src io.Reader) (Decoder, error)
}

type RestoreFromReaderArgs struct {
	RestoreArgs
}

type RestoreFromReaderStatus struct {
	RestoreStatus
}

type RestoreFromReaderHandler struct {
	status       RestoreFromReaderStatus
	args         RestoreFromReaderArgs
	dec          DecoderFactory
	readers      []io.Reader
	workerErrors <-chan error
	restoreHandler
}

func NewRestoreFromReaderHandler(args RestoreFromReaderArgs, ac *a.Client, dec DecoderFactory, readers []io.Reader) *RestoreFromReaderHandler {
	workerErrors := make(chan error)
	restoreHandler := newRestoreHandler(args.RestoreArgs, ac, workerErrors)

	return &RestoreFromReaderHandler{
		args:           args,
		dec:            dec,
		readers:        readers,
		workerErrors:   workerErrors,
		restoreHandler: *restoreHandler,
	}
}

// TODO don't expose this by moving it to the backuplib package
// we don't want users calling Run directly
func (rrh *RestoreFromReaderHandler) Run(readers []io.Reader) <-chan error {
	errors := make(chan error)

	go func(errChan chan<- error) {
		defer rrh.Close()
		defer close(errChan)

		for _, reader := range readers {

			numDataReaders := rrh.args.Parallel
			dataReaders := make([]datahandlers.DataReader, numDataReaders)

			for i := 0; i < numDataReaders; i++ {
				decoder, err := rrh.dec.CreateDecoder(reader)
				if err != nil {
					errChan <- err
					return
				}

				dataReaders[i] = datahandlers.NewGenericReader(decoder)
			}

			rrh.restoreHandler.Run(dataReaders)

			err := <-rrh.workerErrors
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
