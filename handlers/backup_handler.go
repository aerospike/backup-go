package handlers

import (
	datahandlers "backuplib/data_handlers"
	"errors"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type BackupArgs struct {
	Mode      WorkMode
	Namespace string
	Set       string
	Parallel  int
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
	// TODO this should be a backuplib client which means handlers need to move to the backuplib package
	AeroClient *a.Client
	jobHandler
}

func NewBackupHandler(writers []datahandlers.DataWriter, args BackupArgs, ac *a.Client) (*BackupHandler, error) {
	readers := make([]datahandlers.DataReader, args.Parallel)
	for i := 0; i < args.Parallel; i++ {
		for i := 0; i < args.Parallel; i++ {
			var first bool
			if i == 0 {
				first = true
			}

			begin := (i * PARTITIONS) / args.Parallel
			count := PARTITIONS / args.Parallel // TODO verify no off by 1 error

			ARCFG := &datahandlers.ARConfig{
				Namespace:      args.Namespace,
				Set:            args.Set,
				FirstPartition: begin,
				NumPartitions:  count,
				First:          first,
			}

			dataReader, err := datahandlers.NewAerospikeReader(
				ARCFG,
				ac,
			)
			if err != nil {
				return nil, err
			}

			readers[i] = dataReader
		}
	}

	processors := make([]datahandlers.DataProcessor, args.Parallel)
	for i := 0; i < args.Parallel; i++ {
		processor := datahandlers.NewNOOPProcessor()
		processors[i] = processor
	}

	pipeline := datahandlers.NewDataPipeline(
		readers,
		processors,
		writers,
	)

	return &BackupHandler{
		args:       args,
		errors:     make(chan error),
		jobHandler: jobHandler{pipeline: pipeline},
		AeroClient: ac,
	}, nil
}

func (o *BackupHandler) GetStats() (BackupStatus, error) {
	return BackupStatus{}, errors.New("UNIMPLEMENTED")
}
