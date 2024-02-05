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
	worker     workHandler
}

func newRestoreHandler(args RestoreArgs, ac *a.Client) *restoreHandler {
	wh := newWorkHandler()

	return &restoreHandler{
		args:       args,
		aeroClient: ac,
		worker:     *wh,
	}
}

func (rh *restoreHandler) Run(readers []DataReader) error {

	processors := make([]DataProcessor, rh.args.Parallel)
	for i := 0; i < rh.args.Parallel; i++ {
		processor := datahandlers.NewNOOPProcessor()
		processors[i] = processor
	}

	writers := make([]DataWriter, rh.args.Parallel)
	for i := 0; i < rh.args.Parallel; i++ {
		writer := datahandlers.NewRestoreWriter(rh.aeroClient)
		writers[i] = writer
	}

	job := NewDataPipeline(
		readers,
		processors,
		writers,
	)

	return rh.worker.doJob(job)
}

func (*restoreHandler) GetStats() (RestoreStatus, error) {
	return RestoreStatus{}, errors.New("UNIMPLEMENTED")
}
