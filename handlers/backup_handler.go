package handlers

import (
	datahandlers "backuplib/data_handlers"
	"errors"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type BackupOpts struct {
	Set      string
	Parallel int
}

type BackupStatus struct {
	Active      bool
	RecordCount int
	Mode        WorkMode
}

type backupHandler struct {
	namespace string
	status    BackupStatus
	opts      BackupOpts
	// TODO this should be a backuplib client which means handlers need to move to the backuplib package
	aeroClient *a.Client
	worker     workHandler
}

func newBackupHandler(args BackupOpts, ac *a.Client, namespace string) *backupHandler {
	wh := newWorkHandler()

	handler := &backupHandler{
		namespace:  namespace,
		opts:       args,
		aeroClient: ac,
		worker:     *wh,
	}

	return handler
}

// TODO don't expose this by moving it to the backuplib package
// We don't want users calling Run directly
func (bh *backupHandler) Run(writers []DataWriter) error {
	readers := make([]DataReader, bh.opts.Parallel)
	for i := 0; i < bh.opts.Parallel; i++ {
		var first bool
		if i == 0 {
			first = true
		}

		begin := (i * PARTITIONS) / bh.opts.Parallel
		count := PARTITIONS / bh.opts.Parallel // TODO verify no off by 1 error

		ARCFG := &datahandlers.ARConfig{
			Namespace:      bh.namespace,
			Set:            bh.opts.Set,
			FirstPartition: begin,
			NumPartitions:  count,
			First:          first,
		}

		dataReader := datahandlers.NewAerospikeReader(
			ARCFG,
			bh.aeroClient,
		)

		readers[i] = dataReader
	}

	processors := make([]DataProcessor, bh.opts.Parallel)
	for i := 0; i < bh.opts.Parallel; i++ {
		processor := datahandlers.NewNOOPProcessor()
		processors[i] = processor
	}

	job := NewDataPipeline(
		readers,
		processors,
		writers,
	)

	return bh.worker.doJob(job)
}

func (bh *backupHandler) GetStats() (BackupStatus, error) {
	return BackupStatus{}, errors.New("UNIMPLEMENTED")
}
