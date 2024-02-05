package handlers

import (
	datahandlers "backuplib/data_handlers"
	"errors"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type BackupArgs struct {
	Namespace string
	Set       string
	Parallel  int
}

type BackupStatus struct {
	Active      bool
	RecordCount int
	Mode        WorkMode
}

type backupHandler struct {
	status BackupStatus
	args   BackupArgs
	// TODO this should be a backuplib client which means handlers need to move to the backuplib package
	aeroClient *a.Client
	jobs       chan<- *datahandlers.DataPipeline
	workHandler
}

func newBackupHandler(args BackupArgs, ac *a.Client, errors chan<- error) *backupHandler {
	jobs := make(chan *datahandlers.DataPipeline)
	wh := NewWorkHandler(jobs, errors)

	handler := &backupHandler{
		args:        args,
		aeroClient:  ac,
		jobs:        jobs,
		workHandler: *wh,
	}

	return handler
}

// TODO don't expose this by moving it to the backuplib package
// We don't want users calling Run directly
func (bh *backupHandler) Run(writers []datahandlers.DataWriter) {
	jobChan := bh.jobs

	readers := make([]datahandlers.DataReader, bh.args.Parallel)
	for i := 0; i < bh.args.Parallel; i++ {
		var first bool
		if i == 0 {
			first = true
		}

		begin := (i * PARTITIONS) / bh.args.Parallel
		count := PARTITIONS / bh.args.Parallel // TODO verify no off by 1 error

		ARCFG := &datahandlers.ARConfig{
			Namespace:      bh.args.Namespace,
			Set:            bh.args.Set,
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

	processors := make([]datahandlers.DataProcessor, bh.args.Parallel)
	for i := 0; i < bh.args.Parallel; i++ {
		processor := datahandlers.NewNOOPProcessor()
		processors[i] = processor
	}

	job := datahandlers.NewDataPipeline(
		readers,
		processors,
		writers,
	)

	jobChan <- job
}

func (bh *backupHandler) Close() {
	close(bh.jobs)
}

func (bh *backupHandler) GetStats() (BackupStatus, error) {
	return BackupStatus{}, errors.New("UNIMPLEMENTED")
}
