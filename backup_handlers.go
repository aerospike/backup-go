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
	"context"
	"io"

	"github.com/aerospike/aerospike-tools-backup-lib/encoding/asb"
	"github.com/aerospike/aerospike-tools-backup-lib/models"
	"github.com/aerospike/aerospike-tools-backup-lib/pipeline"

	a "github.com/aerospike/aerospike-client-go/v7"
)

const (
	PARTITIONS = 4096
)

// **** Base Backup Handler ****

type backupHandlerBase struct {
	namespace       string
	config          *BackupBaseConfig
	aerospikeClient *a.Client
	worker          workHandler
}

func newBackupHandlerBase(config *BackupBaseConfig, ac *a.Client, namespace string) *backupHandlerBase {
	wh := newWorkHandler()

	handler := &backupHandlerBase{
		namespace:       namespace,
		config:          config,
		aerospikeClient: ac,
		worker:          *wh,
	}

	return handler
}

func (bh *backupHandlerBase) run(writers []*WriteWorker[*models.Token]) error {
	readWorkers := make([]pipeline.Worker[*models.Token], bh.config.Parallel)
	for i := 0; i < bh.config.Parallel; i++ {
		begin := (i * PARTITIONS) / bh.config.Parallel
		count := PARTITIONS / bh.config.Parallel // TODO verify no off by 1 error

		ARRCFG := &ARRConfig{
			Namespace:      bh.namespace,
			Set:            bh.config.Set,
			FirstPartition: begin,
			NumPartitions:  count,
		}

		recordReader := NewAerospikeRecordReader(
			ARRCFG,
			bh.aerospikeClient,
		)

		readWorkers[i] = NewReadWorker(recordReader)
	}

	processorWorkers := make([]pipeline.Worker[*models.Token], bh.config.Parallel)
	for i := 0; i < bh.config.Parallel; i++ {
		processor := NewNoOpProcessor()
		processorWorkers[i] = NewProcessorWorker(processor)
	}

	writeWorkers := make([]pipeline.Worker[*models.Token], len(writers))
	for i, w := range writers {
		writeWorkers[i] = w
	}

	job := pipeline.NewPipeline[*models.Token](
		readWorkers,
		processorWorkers,
		writeWorkers,
	)

	return bh.worker.DoJob(job)
}

// **** Backup To Writer Handler ****

// BackupStatus stores the status of a backup job
type BackupStatus struct{}

// BackupHandler handles a backup job to a set of io.writers
type BackupHandler struct {
	status  *BackupStatus
	config  *BackupConfig
	writers []io.Writer
	errors  chan error
	backupHandlerBase
}

// newBackupHandler creates a new BackupHandler
func newBackupHandler(config *BackupConfig, ac *a.Client, writers []io.Writer) *BackupHandler {
	namespace := config.Namespace
	backupHandler := newBackupHandlerBase(&config.BackupBaseConfig, ac, namespace)

	return &BackupHandler{
		config:            config,
		writers:           writers,
		backupHandlerBase: *backupHandler,
	}
}

// run runs the backup job
// currently this should only be run once
func (bwh *BackupHandler) run(writers []io.Writer) {
	bwh.errors = make(chan error)

	go func(errChan chan<- error) {

		// NOTE: order is important here
		// if we close the errChan before we handle the panic
		// the panic will attempt to send on a closed channel
		defer close(errChan)
		defer handlePanic(errChan)

		batchSize := bwh.config.Parallel
		dataWriters := []*WriteWorker[*models.Token]{}

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

			err = bwh.backupHandlerBase.run(dataWriters)
			if err != nil {
				errChan <- err
				return
			}

			clear(dataWriters)
		}

	}(bwh.errors)
}

// GetStats returns the stats of the backup job
func (bwh *BackupHandler) GetStats() BackupStatus {
	return *bwh.status
}

// Wait waits for the backup job to complete and returns an error if the job failed
func (bwh *BackupHandler) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-bwh.errors:
		return err
	}
}

func getDataWriter(eb EncoderBuilder, w io.Writer, namespace string, first bool) (*WriteWorker[*models.Token], error) {
	enc, err := eb.CreateEncoder()
	if err != nil {
		return nil, err
	}

	switch encT := enc.(type) {
	case *asb.Encoder:
		asbw := NewASBWriter(encT, w)
		err := asbw.Init(namespace, first)
		if err != nil {
			return nil, err
		}

		worker := NewWriteWorker(asbw)

		return worker, err

	default:
		gw := NewGenericWriter(encT, w)
		worker := NewWriteWorker(gw)

		return worker, nil
	}
}
