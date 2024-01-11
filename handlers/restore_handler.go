package handlers

import (
	"backuplib/workers"
	"errors"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type RestoreUnmarshaller interface {
	UnmarshalRecord([]byte) (*a.Record, error)
}

type RestoreArgs struct {
	Namespace    string
	Set          string
	UnMarshaller RestoreUnmarshaller
	Parallel     int
	FilePath     string
	DirPath      string
	// TODO S3Path
}

type RestoreStatus struct {
	active      bool
	recordCount int
	mode        WorkMode
}

type RestoreHandler struct {
	workers []*workers.RestoreWorker
	status  RestoreStatus
	args    RestoreArgs
	errors  chan error
	workHandler
}

func NewRestoreHandler(workers []*workers.RestoreWorker, args RestoreArgs) (*RestoreHandler, error) {

	workerImplementations := make([]Worker, len(workers))
	for i, w := range workers {
		workerImplementations[i] = Worker(w)
	}

	return &RestoreHandler{
		workers:     workers, // TODO should both this and workHandler have a list of workers?
		args:        args,
		errors:      make(chan error, len(workers)),
		workHandler: *NewWorkHandler(workerImplementations),
	}, nil
}

func (o *RestoreHandler) GetStats() (RestoreStatus, error) {
	return RestoreStatus{}, errors.New("UNIMPLEMENTED")
}
