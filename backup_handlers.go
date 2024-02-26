// Copyright 2024-2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backuplib

import (
	"io"

	datahandlers "github.com/aerospike/aerospike-tools-backup-lib/data_handlers"
	"github.com/aerospike/aerospike-tools-backup-lib/encoding/asb"
	"github.com/aerospike/aerospike-tools-backup-lib/pipeline"

	a "github.com/aerospike/aerospike-client-go/v7"
)

const (
	PARTITIONS = 4096
)

// **** Generic Backup Handler ****

// BackupStatus is the status of a backup job
// TODO fill this out
type BackupStatus struct{}

type backupHandler struct {
	namespace       string
	config          *BackupBaseConfig
	aerospikeClient *a.Client
	worker          workHandler
}

func newBackupHandler(config *BackupBaseConfig, ac *a.Client, namespace string) *backupHandler {
	wh := newWorkHandler()

	handler := &backupHandler{
		namespace:       namespace,
		config:          config,
		aerospikeClient: ac,
		worker:          *wh,
	}

	return handler
}

// TODO change the any typed pipeline to a message or token type
func (bh *backupHandler) run(writers []*datahandlers.WriteWorker) error {
	readWorkers := make([]pipeline.Worker[any], bh.config.Parallel)
	for i := 0; i < bh.config.Parallel; i++ {
		begin := (i * PARTITIONS) / bh.config.Parallel
		count := PARTITIONS / bh.config.Parallel // TODO verify no off by 1 error

		ARRCFG := &datahandlers.ARRConfig{
			Namespace:      bh.namespace,
			Set:            bh.config.Set,
			FirstPartition: begin,
			NumPartitions:  count,
		}

		dataReader := datahandlers.NewAerospikeRecordReader(
			ARRCFG,
			bh.aerospikeClient,
		)

		readWorkers[i] = datahandlers.NewReadWorker[any](dataReader)
	}

	// TODO change the any typed pipeline to a message or token type
	processorWorkers := make([]pipeline.Worker[any], bh.config.Parallel)
	for i := 0; i < bh.config.Parallel; i++ {
		processor := datahandlers.NewNOOPProcessor()
		processorWorkers[i] = datahandlers.NewProcessorWorker(processor)
	}

	writeWorkers := make([]pipeline.Worker[any], len(writers))
	for i, w := range writers {
		writeWorkers[i] = w
	}

	job := pipeline.NewPipeline(
		readWorkers,
		processorWorkers,
		writeWorkers,
	)

	return bh.worker.DoJob(job)
}

// **** Backup To Writer Handler ****

// BackupToWriterStatus stores the status of a backup to writer job
type BackupToWriterStatus struct {
	BackupStatus
}

// BackupToWriterHandler handles a backup job to a set of io.writers
type BackupToWriterHandler struct {
	status  *BackupToWriterStatus
	config  *BackupToWriterConfig
	writers []io.Writer
	errors  chan error
	backupHandler
}

func newBackupToWriterHandler(config *BackupToWriterConfig, ac *a.Client, writers []io.Writer) *BackupToWriterHandler {
	namespace := config.Namespace
	backupHandler := newBackupHandler(&config.BackupBaseConfig, ac, namespace)

	return &BackupToWriterHandler{
		config:        config,
		writers:       writers,
		backupHandler: *backupHandler,
	}
}

// run runs the backup job
// currently this should only be run once
func (bwh *BackupToWriterHandler) run(writers []io.Writer) {
	bwh.errors = make(chan error)

	go func(errChan chan<- error) {

		// NOTE: order is important here
		// if we close the errChan before we handle the panic
		// the panic will attempt to send on a closed channel
		defer close(errChan)
		defer handlePanic(errChan)

		batchSize := bwh.config.Parallel
		// TODO change the any typed pipeline to a message or token type
		dataWriters := []*datahandlers.WriteWorker{}

		for i, writer := range writers {

			dw, err := getDataWriter(bwh.config.EncoderBuilder, writer, bwh.namespace, i == 0)
			if err != nil {
				errChan <- err
				return
			}

			dataWriters = append(dataWriters, dw)
			// if we have not reached the batch size and we have more writers
			// continue to the next writer
			// if we are at the end of writers then run no matter what
			if i < len(writers)-1 && len(dataWriters) < batchSize {
				continue
			}

			err = bwh.backupHandler.run(dataWriters)
			if err != nil {
				errChan <- err
				return
			}

			clear(dataWriters)
		}

	}(bwh.errors)
}

// GetStats returns the stats of the backup job
func (bwh *BackupToWriterHandler) GetStats() BackupToWriterStatus {
	return *bwh.status
}

// Wait waits for the backup job to complete and returns an error if the job failed
func (bwh *BackupToWriterHandler) Wait() error {
	return <-bwh.errors
}

// TODO change the any typed pipeline to a message or token type
func getDataWriter(eb EncoderBuilder, w io.Writer, namespace string, first bool) (*datahandlers.WriteWorker, error) {
	enc, err := eb.CreateEncoder()
	if err != nil {
		return nil, err
	}

	switch encT := enc.(type) {
	case *asb.Encoder:
		asbw := datahandlers.NewASBWriter(encT, w)
		err := asbw.Init(namespace, first)
		if err != nil {
			return nil, err
		}

		worker := datahandlers.NewWriteWorker(asbw)

		return worker, err

	default:
		gw := datahandlers.NewGenericWriter(encT, w)
		worker := datahandlers.NewWriteWorker(gw)

		return worker, nil
	}
}
