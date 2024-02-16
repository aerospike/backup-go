package backuplib

import (
	datahandlers "backuplib/data_handlers"
	"backuplib/encoder"
	"errors"
	"io"

	a "github.com/aerospike/aerospike-client-go/v7"
)

const (
	PARTITIONS = 4096
)

// **** Generic Backup Handler ****

// TODO fill this out
type BackupStatus struct{}

type backupHandler struct {
	namespace       string
	status          *BackupStatus
	config          *BackupBaseConfig
	aerospikeClient *a.Client
	worker          workHandler
}

func newBackupHandler(config *BackupBaseConfig, ac *a.Client, namespace string) *backupHandler {
	wh := newWorkHandler()

	handler := &backupHandler{
		namespace:       namespace,
		config:          config,
		aerospikeClient: ac,
		worker:          *wh,
	}

	return handler
}

func (bh *backupHandler) run(writers []datahandlers.Writer) error {
	readers := make([]datahandlers.Reader, bh.config.Parallel)
	for i := 0; i < bh.config.Parallel; i++ {
		begin := (i * PARTITIONS) / bh.config.Parallel
		count := PARTITIONS / bh.config.Parallel // TODO verify no off by 1 error

		ARRCFG := &datahandlers.ARRConfig{
			Namespace:      bh.namespace,
			Set:            bh.config.Set,
			FirstPartition: begin,
			NumPartitions:  count,
		}

		dataReader := datahandlers.NewAerospikeRecordReader(
			ARRCFG,
			bh.aerospikeClient,
		)

		readers[i] = dataReader
	}

	processors := make([]datahandlers.Processor, bh.config.Parallel)
	for i := 0; i < bh.config.Parallel; i++ {
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

type BackupToWriterStatus struct {
	BackupStatus
}

type BackupToWriterHandler struct {
	status  *BackupToWriterStatus
	config  *BackupToWriterConfig
	writers []io.Writer
	errors  chan error
	backupHandler
}

func newBackupToWriterHandler(config *BackupToWriterConfig, ac *a.Client, writers []io.Writer) *BackupToWriterHandler {
	namespace := config.Namespace
	backupHandler := newBackupHandler(&config.BackupBaseConfig, ac, namespace)

	return &BackupToWriterHandler{
		config:        config,
		writers:       writers,
		backupHandler: *backupHandler,
	}
}

// run runs the backup job
// currently this should only be run once
func (bwh *BackupToWriterHandler) run(writers []io.Writer) {
	bwh.errors = make(chan error)

	go func(errChan chan<- error) {
		defer close(errChan)

		batchSize := bwh.config.Parallel
		dataWriters := []datahandlers.Writer{}

		for i, writer := range writers {

			dw, err := getDataWriter(bwh.config.EncoderBuilder, writer, bwh.namespace, i == 0)
			if err != nil {
				errChan <- err
				return
			}

			dataWriters = append(dataWriters, dw)
			// if we have not reached the batch size and we have more writers
			// continue to the next writer
			// if we are at the end of writers then run no matter what
			if i < len(writers)-1 && len(dataWriters) < batchSize {
				continue
			}

			err = bwh.backupHandler.run(dataWriters)
			if err != nil {
				errChan <- err
				return
			}

			clear(dataWriters)
		}

	}(bwh.errors)
}

// GetStats returns the stats of the backup job
func (bwh *BackupToWriterHandler) GetStats() (BackupToWriterStatus, error) {
	return BackupToWriterStatus{}, errors.New("UNIMPLEMENTED")
}

// Wait waits for the restore job to complete and returns an error if the job failed
func (bwh *BackupToWriterHandler) Wait() error {
	return <-bwh.errors
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
