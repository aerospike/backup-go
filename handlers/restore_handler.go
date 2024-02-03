package handlers

import (
	datahandlers "backuplib/data_handlers"
	"errors"
)

type RestoreArgs struct {
	Mode WorkMode
}

type RestoreStatus struct {
	Active      bool
	RecordCount int
	Mode        WorkMode
}

type RestoreHandler struct {
	status RestoreStatus
	args   RestoreArgs
	errors chan error // TODO what buffer size should this have?
	jobHandler
}

func NewRestoreHandler(pipeline *datahandlers.DataPipeline, args RestoreArgs) (*RestoreHandler, error) {
	return &RestoreHandler{
		args:       args,
		errors:     make(chan error),
		jobHandler: jobHandler{pipeline: pipeline},
	}, nil
}

func (o *RestoreHandler) GetStats() (RestoreStatus, error) {
	return RestoreStatus{}, errors.New("UNIMPLEMENTED")
}
