package handlers

import (
	datahandlers "backuplib/data_handlers"
	"errors"
)

type BackupArgs struct {
	Mode WorkMode
}

type BackupStatus struct {
	Active      bool
	RecordCount int
	Mode        WorkMode
}

type BackupHandler struct {
	status BackupStatus
	args   BackupArgs
	errors chan error // TODO what buffer size should this have?
	jobHandler
}

func NewBackupHandler(pipeline *datahandlers.DataPipeline, args BackupArgs) (*BackupHandler, error) {
	return &BackupHandler{
		args:       args,
		errors:     make(chan error),
		jobHandler: jobHandler{pipeline: pipeline},
	}, nil
}

func (o *BackupHandler) GetStats() (RestoreStatus, error) {
	return RestoreStatus{}, errors.New("UNIMPLEMENTED")
}
