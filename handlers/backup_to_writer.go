package handlers

import (
	datahandlers "backuplib/data_handlers"
	"backuplib/models"
	"errors"
	"io"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type Encoder interface {
	EncodeMetadata(v models.Metadata) ([]byte, error)
	EncodeRecord(v models.Record) ([]byte, error)
	EncodeUDF(v models.UDF) ([]byte, error)
	EncodeSIndex(v models.SecondaryIndex) ([]byte, error)
}

type EncoderFactory interface {
	CreateEncoder() (Encoder, error)
}

type BackupToWriterArgs struct {
	BackupArgs
}

type BackupToWriterStatus struct {
	BackupStatus
}

type BackupToWriterHandler struct {
	status  BackupToWriterStatus
	args    BackupToWriterArgs
	writers []io.Writer
	BackupHandler
}

func NewBackupToWriterHandler(args BackupToWriterArgs, ac *a.Client, enc EncoderFactory, writers []io.Writer) (*BackupToWriterHandler, error) {

	dataWriters := make([]datahandlers.DataWriter, len(writers))
	for i, writer := range writers {
		encoder, err := enc.CreateEncoder()
		if err != nil {
			return nil, err
		}

		dataWriters[i] = datahandlers.NewGenericWriter(encoder, writer)
	}

	backupHandler, err := NewBackupHandler(dataWriters, args.BackupArgs, ac)
	if err != nil {
		return nil, err
	}

	return &BackupToWriterHandler{
		args:          args,
		writers:       writers,
		BackupHandler: *backupHandler,
	}, nil
}

func (o *BackupToWriterHandler) GetStats() (BackupToWriterStatus, error) {
	return BackupToWriterStatus{}, errors.New("UNIMPLEMENTED")
}
