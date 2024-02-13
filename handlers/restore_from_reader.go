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

type DecoderBuilder interface {
	CreateDecoder() (Decoder, error)
	SetSource(src io.Reader)
}

type RestoreFromReaderArgs struct {
	RestoreArgs
}

type RestoreFromReaderStatus struct {
	RestoreStatus
}

type RestoreFromReaderHandler struct {
	status  RestoreFromReaderStatus
	args    RestoreFromReaderArgs
	dec     DecoderBuilder
	readers []io.Reader
	restoreHandler
}

func NewRestoreFromReaderHandler(args RestoreFromReaderArgs, ac *a.Client, dec DecoderBuilder, readers []io.Reader) *RestoreFromReaderHandler {
	restoreHandler := newRestoreHandler(args.RestoreArgs, ac)

	return &RestoreFromReaderHandler{
		args:           args,
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
