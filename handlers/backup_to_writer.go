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

type BackupToWriterArgs struct {
	BackupArgs
}

type BackupToWriterStatus struct {
	BackupStatus
}

type BackupToWriterHandler struct {
	status       BackupToWriterStatus
	args         BackupToWriterArgs
	enc          EncoderFactory
	writers      []io.Writer
	workerErrors <-chan error
	backupHandler
}

func NewBackupToWriterHandler(args BackupToWriterArgs, ac *a.Client, enc EncoderFactory, writers []io.Writer) *BackupToWriterHandler {
	workerErrors := make(chan error)
	backupHandler := newBackupHandler(args.BackupArgs, ac, workerErrors)

	return &BackupToWriterHandler{
		args:          args,
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
		defer bwh.Close()
		defer close(errChan)

		for _, writer := range writers {

			numDataWriters := bwh.args.Parallel
			dataWriters := make([]datahandlers.DataWriter, numDataWriters)

			for i := 0; i < numDataWriters; i++ {
				encoder := bwh.enc.CreateEncoder()
				dataWriters[i] = datahandlers.NewGenericWriter(encoder, writer)
			}

			bwh.backupHandler.Run(dataWriters)

			select {
			case err := <-bwh.workerErrors:
				if err != nil {
					errChan <- err
					return
				}
			default:
				continue
			}
		}
	}(errors)

	return errors
}

func (bwh *BackupToWriterHandler) GetStats() (BackupToWriterStatus, error) {
	return BackupToWriterStatus{}, errors.New("UNIMPLEMENTED")
}
