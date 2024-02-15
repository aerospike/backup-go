package backuplib

import (
	datahandlers "backuplib/data_handlers"
	"backuplib/encoder"
	infoclient "backuplib/info_client"
	"errors"
	"io"

	a "github.com/aerospike/aerospike-client-go/v7"
)

const (
	PARTITIONS = 4096
)

// **** Generic Backup Handler ****

type AerospikePolicies struct {
	InfoPolicy *a.InfoPolicy
}

type BackupOpts struct {
	Set      string
	Parallel int
	Policies AerospikePolicies
}

type BackupStatus struct {
	Active      bool
	RecordCount int
}

type backupHandler struct {
	namespace string
	status    BackupStatus
	opts      BackupOpts
	// TODO this should be a backuplib client which means handlers need to move to the backuplib package
	dbClient DBBackupClient
	worker   workHandler
}

type DBBackupClient interface {
	datahandlers.Scanner
	infoclient.InfoGetter
}

func newBackupHandler(args BackupOpts, ac DBBackupClient, namespace string) *backupHandler {
	wh := newWorkHandler()

	handler := &backupHandler{
		namespace: namespace,
		opts:      args,
		dbClient:  ac,
		worker:    *wh,
	}

	return handler
}

func (bh *backupHandler) run(writers []datahandlers.Writer) error {
	readers := make([]datahandlers.Reader, bh.opts.Parallel)
	for i := 0; i < bh.opts.Parallel; i++ {
		begin := (i * PARTITIONS) / bh.opts.Parallel
		count := PARTITIONS / bh.opts.Parallel // TODO verify no off by 1 error

		ARCFG := &datahandlers.ARRConfig{
			Namespace:      bh.namespace,
			Set:            bh.opts.Set,
			FirstPartition: begin,
			NumPartitions:  count,
		}

		dataReader := datahandlers.NewAerospikeRecordReader(
			ARCFG,
			bh.dbClient,
		)

		readers[i] = dataReader
	}

	processors := make([]datahandlers.Processor, bh.opts.Parallel)
	for i := 0; i < bh.opts.Parallel; i++ {
		processor := datahandlers.NewNOOPProcessor()
		processors[i] = processor
	}

	job := datahandlers.NewDataPipeline(
		readers,
		processors,
		writers,
	)

	return bh.worker.DoJob(job)
}

func (bh *backupHandler) GetStats() (BackupStatus, error) {
	return BackupStatus{}, errors.New("UNIMPLEMENTED")
}

// **** Backup To Writer Handler ****

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

func newBackupToWriterHandler(opts BackupToWriterOpts, ac DBBackupClient, enc EncoderBuilder, namespace string, writers []io.Writer) *BackupToWriterHandler {
	backupHandler := newBackupHandler(opts.BackupOpts, ac, namespace)

	return &BackupToWriterHandler{
		opts:          opts,
		enc:           enc,
		writers:       writers,
		backupHandler: *backupHandler,
	}
}

func (bwh *BackupToWriterHandler) run(writers []io.Writer) <-chan error {
	errors := make(chan error)

	go func(errChan chan<- error) {
		defer close(errChan)

		for i, writer := range writers {

			numDataWriters := bwh.opts.Parallel
			dataWriters := make([]datahandlers.Writer, numDataWriters)

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

				err = bwh.worker.DoJob(oneShotJob)
				if err != nil {
					errChan <- err
					return
				}
			}

			err := bwh.backupHandler.run(dataWriters)
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

func (bwh *BackupToWriterHandler) createOneShotPipeline(dw datahandlers.Writer) (*datahandlers.DataPipeline, error) {
	// SIndex and UDF work is done "first" because parallelizing it
	// would scramble which writers receive the data
	firstWriter := dw
	firstProcessor := datahandlers.NewNOOPProcessor()

	infoclient := infoclient.NewInfoClient(bwh.dbClient, bwh.opts.Policies.InfoPolicy)

	// TODO add UDF reader
	firstReader := datahandlers.NewSIndexReader(infoclient, bwh.namespace)

	firstJob := datahandlers.NewDataPipeline(
		[]datahandlers.Reader{firstReader},
		[]datahandlers.Processor{firstProcessor},
		[]datahandlers.Writer{firstWriter},
	)

	return firstJob, nil
}

func getDataWriter(eb EncoderBuilder, w io.Writer, namespace string, first bool) (datahandlers.Writer, error) {
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
