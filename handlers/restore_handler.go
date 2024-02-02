package handlers

import (
	datahandlers "backuplib/data_handlers"
	"errors"
)

type RestoreArgs struct {
	Parallel int
}

type RestoreStatus struct {
	active      bool
	recordCount int
	mode        WorkMode
}

type RestoreHandler struct {
	workers []*datahandlers.DataPipeline
	status  RestoreStatus
	args    RestoreArgs
	errors  chan error
	jobHandler
}

func NewRestoreHandler(workers []*datahandlers.DataPipeline, args RestoreArgs) (*RestoreHandler, error) {

	workerImplementations := make([]Job, len(workers))
	for i, w := range workers {
		workerImplementations[i] = Job(w)
	}

	return &RestoreHandler{
		workers:    workers, // TODO should both this and workHandler have a list of workers?
		args:       args,
		errors:     make(chan error, len(workers)),
		jobHandler: *NewWorkHandler(workerImplementations, args.Parallel),
	}, nil
}

func (o *RestoreHandler) GetStats() (RestoreStatus, error) {
	return RestoreStatus{}, errors.New("UNIMPLEMENTED")
}

// type restoreType int

// const (
// 	restoreTypeFile restoreType = iota
// 	restoreTypeDir
// )

// type dataFormat int

// const (
// 	dataFormatASB dataFormat = iota
// )

// type restorePipelineFactory struct {
// 	restoreType restoreType
// 	filepath    string
// 	dirpath     string
// 	numPipes    int
// }

// func NewRestorePipelineFactory(restoreType restoreType, filepath, dirpath string, numPipes int) *restorePipelineFactory {
// 	return &restorePipelineFactory{
// 		restoreType: restoreType,
// 		filepath:    filepath,
// 		dirpath:     dirpath,
// 		numPipes:    numPipes,
// 	}
// }

// func (o *restorePipelineFactory) CreatePipelines() ([]*datahandlers.DataPipeline, error) {
// 	workers := make([]*datahandlers.DataPipeline, o.numPipes)

// 	for i := 0; i < o.numPipes; i++ {
// 		var dp *datahandlers.DataPipeline

// 		switch restoreType {
// 		case restoreTypeFile:
// 			dp = datahandlers.NewDataPipeline(
// 				datahandlers.NewReader(
// 					decoder.NewASBReader(),
// 				),
// 			)
// 		}

// 		workers[i] = &dp
// 	}
// }
