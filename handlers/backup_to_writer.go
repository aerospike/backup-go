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
	status  BackupToWriterStatus
	opts    BackupToWriterOpts
	enc     EncoderFactory
	writers []io.Writer
	backupHandler
}

func NewBackupToWriterHandler(args BackupToWriterOpts, ac *a.Client, enc EncoderFactory, namespace string, writers []io.Writer) *BackupToWriterHandler {
	backupHandler := newBackupHandler(args.BackupOpts, ac, namespace)

	return &BackupToWriterHandler{
		opts:          args,
		enc:           enc,
		writers:       writers,
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

			numDataWriters := bwh.opts.Parallel
			dataWriters := make([]DataWriter, numDataWriters)

			for i := 0; i < numDataWriters; i++ {
				encoder := bwh.enc.CreateEncoder()
				dataWriters[i] = datahandlers.NewGenericWriter(encoder, writer)
			}

			err := bwh.backupHandler.Run(dataWriters)
			if err != nil {
				errChan <- err
				return
			}
		}

	}(errors)

	return errors
}

func (bwh *BackupToWriterHandler) GetStats() (BackupToWriterStatus, error) {
	return BackupToWriterStatus{}, errors.New("UNIMPLEMENTED")
}
