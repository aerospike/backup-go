package handlers

import (
	datahandlers "backuplib/data_handlers"
	"errors"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type RestoreArgs struct {
	Parallel int
}

type RestoreStatus struct {
	Active      bool
	RecordCount int
	Mode        WorkMode
}

type restoreHandler struct {
	status     RestoreStatus
	args       RestoreArgs
	aeroClient *a.Client
	jobs       chan<- *datahandlers.DataPipeline
	workHandler
}

func newRestoreHandler(args RestoreArgs, ac *a.Client, errors chan<- error) *restoreHandler {
	jobs := make(chan *datahandlers.DataPipeline)
	wh := NewWorkHandler(jobs, errors)

	return &restoreHandler{
		args:        args,
		aeroClient:  ac,
		jobs:        jobs,
		workHandler: *wh,
	}
}

func (rh *restoreHandler) Run(readers []datahandlers.DataReader) {

	processors := make([]datahandlers.DataProcessor, rh.args.Parallel)
	for i := 0; i < rh.args.Parallel; i++ {
		processor := datahandlers.NewNOOPProcessor()
		processors[i] = processor
	}

	writers := make([]datahandlers.DataWriter, rh.args.Parallel)
	for i := 0; i < rh.args.Parallel; i++ {
		writer := datahandlers.NewRestoreWriter(rh.aeroClient)
		writers[i] = writer
	}

	job := datahandlers.NewDataPipeline(
		readers,
		processors,
		writers,
	)

	rh.jobs <- job
}

func (rh *restoreHandler) Close() {
	close(rh.jobs)
}

func (*restoreHandler) GetStats() (RestoreStatus, error) {
	return RestoreStatus{}, errors.New("UNIMPLEMENTED")
}
