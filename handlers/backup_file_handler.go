package handlers

import (
	"errors"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type BackupFileArgs struct {
	BackupArgs
}

type BackupFileStatus struct {
	BackupStatus
}

type BackupFileHandler struct {
	status   BackupFileStatus
	args     BackupFileArgs
	filePath string
	BackupHandler
}

func NewBackupFileHandler(args BackupFileArgs, ac *a.Client, enc EncoderFactory, filePath string) (*BackupFileHandler, error) {

	backupHandler := NewBackupHandler(args.BackupArgs, ac)

	return &BackupFileHandler{
		args:          args,
		filePath:      filePath,
		BackupHandler: BackupHandler{jobHandler: jobHandler{pipeline: pipeline}},
	}, nil
}

func (o *BackupFileHandler) GetStats() (BackupFileStatus, error) {
	return BackupFileStatus{}, errors.New("UNIMPLEMENTED")
}
