package handlers

import (
	datahandlers "backuplib/data_handlers"
	"backuplib/encoder"
	infoclient "backuplib/info_client"
	"backuplib/models"
	"errors"
	"io"
	"time"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type Encoder interface {
	EncodeRecord(v *models.Record) ([]byte, error)
	EncodeUDF(v *models.UDF) ([]byte, error)
	EncodeSIndex(v *models.SecondaryIndex) ([]byte, error)
}

type EncoderBuilder interface {
	CreateEncoder() (Encoder, error)
	SetDestination(dest io.Writer)
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
	enc     EncoderBuilder
	writers []io.Writer
	backupHandler
}

func NewBackupToWriterHandler(args BackupToWriterOpts, ac *a.Client, enc EncoderBuilder, namespace string, writers []io.Writer) *BackupToWriterHandler {
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

		for i, writer := range writers {

			numDataWriters := bwh.opts.Parallel
			dataWriters := make([]DataWriter, numDataWriters)

			for j := 0; j < numDataWriters; j++ {
				dw, err := getDataWriter(bwh.enc, writer, bwh.namespace, i == 0)
				if err != nil {
					errChan <- err
					return
				}

				dataWriters[j] = dw
			}

			// run oneshot work for the first writer
			if i == 0 {
				oneShotJob, err := bwh.createOneShotPipeline(dataWriters[0])
				if err != nil {
					errChan <- err
					return
				}

				err = bwh.worker.doJob(oneShotJob)
				if err != nil {
					errChan <- err
					return
				}
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

func (bwh *BackupToWriterHandler) createOneShotPipeline(dw DataWriter) (*DataPipeline, error) {
	// SIndex and UDF work is done "first" because parallelizing it
	// would scramble which writers receive the data
	firstWriter := dw
	firstProcessor := datahandlers.NewNOOPProcessor()

	var timeout time.Duration
	if bwh.opts.Policies.InfoPolicy != nil {
		timeout = bwh.opts.Policies.InfoPolicy.Timeout
	}

	infoOpts := infoclient.InfoClientOpts{
		InfoTimeout: timeout,
	}

	infoclient, err := infoclient.NewInfoClientFromAerospike(bwh.aeroClient, &infoOpts)
	if err != nil {
		return nil, err
	}

	// TODO add UDF reader
	firstReader := datahandlers.NewAerospikeSIndexReader(infoclient, bwh.namespace)

	firstJob := NewDataPipeline(
		[]DataReader{firstReader},
		[]DataProcessor{firstProcessor},
		[]DataWriter{firstWriter},
	)

	return firstJob, nil
}

func getDataWriter(eb EncoderBuilder, w io.Writer, namespace string, first bool) (DataWriter, error) {
	eb.SetDestination(w)
	enc, err := eb.CreateEncoder()
	if err != nil {
		return nil, err
	}

	switch encT := enc.(type) {
	case *encoder.ASBEncoder:
		asbw := datahandlers.NewASBWriter(encT, w)
		asbw.Init(namespace, first)
		return asbw, nil
	default:
		return datahandlers.NewGenericWriter(encT, w), nil
	}
}
