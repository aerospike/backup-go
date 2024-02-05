package handlers

import (
	datahandlers "backuplib/data_handlers"
	"backuplib/models"
	"errors"
	"io"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type Encoder interface {
	EncodeRecord(v *models.Record) ([]byte, error)
	EncodeUDF(v *models.UDF) ([]byte, error)
	EncodeSIndex(v *models.SecondaryIndex) ([]byte, error)
}

type EncoderFactory interface {
	CreateEncoder() Encoder
}

type BackupToWriterOpts struct {
	BackupOpts
}

type BackupToWriterStatus struct {
	BackupStatus
}

type BackupToWriterHandler struct {
	status       BackupToWriterStatus
	opts         BackupToWriterOpts
	enc          EncoderFactory
	writers      []io.Writer
	workerErrors <-chan error
	backupHandler
}

func NewBackupToWriterHandler(args BackupToWriterOpts, ac *a.Client, enc EncoderFactory, namespace string, writers []io.Writer) *BackupToWriterHandler {
	workerErrors := make(chan error)
	backupHandler := newBackupHandler(args.BackupOpts, ac, namespace, workerErrors)

	return &BackupToWriterHandler{
		opts:          args,
		enc:           enc,
		writers:       writers,
		workerErrors:  workerErrors,
		backupHandler: *backupHandler,
	}
}

// TODO don't expose this by moving it to the backuplib package
// we don't want users calling Run directly
// TODO support passing in a context
func (bwh *BackupToWriterHandler) Run(writers []io.Writer) <-chan error {
	errors := make(chan error)

	go func(errChan chan<- error) {
		defer close(errChan)

		for _, writer := range writers {

			// TODO this is kind of messy synchronization,
			// try to find a better way to handle this that doesn't require
			// this non blocking select and the final error check after the loop
			// this also needs to be duplicated in the restore handler
			select {
			case err := <-bwh.workerErrors:
				if err != nil {
					errChan <- err
					return
				}
			default:
			}

			numDataWriters := bwh.opts.Parallel
			dataWriters := make([]datahandlers.DataWriter, numDataWriters)

			for i := 0; i < numDataWriters; i++ {
				encoder := bwh.enc.CreateEncoder()
				dataWriters[i] = datahandlers.NewGenericWriter(encoder, writer)
			}

			bwh.backupHandler.Run(dataWriters)
		}

		bwh.Close()
		err := <-bwh.workerErrors
		if err != nil {
			errChan <- err
		}

	}(errors)

	return errors
}

func (bwh *BackupToWriterHandler) GetStats() (BackupToWriterStatus, error) {
	return BackupToWriterStatus{}, errors.New("UNIMPLEMENTED")
}
