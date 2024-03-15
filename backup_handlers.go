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

package backup

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aerospike/backup-go/encoding"
	"github.com/aerospike/backup-go/encoding/asb"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"

	a "github.com/aerospike/aerospike-client-go/v7"
)

const (
	maxPartitions = 4096
)

// **** Base Backup Handler ****

type backupHandlerBase struct {
	worker          workHandler
	config          *BackupConfig
	aerospikeClient *a.Client
	namespace       string
}

func newBackupHandlerBase(config *BackupConfig, ac *a.Client, namespace string) *backupHandlerBase {
	wh := newWorkHandler()

	handler := &backupHandlerBase{
		namespace:       namespace,
		config:          config,
		aerospikeClient: ac,
		worker:          *wh,
	}

	return handler
}

func (bh *backupHandlerBase) run(ctx context.Context, writers []*writeWorker[*models.Token]) error {
	readWorkers := make([]pipeline.Worker[*models.Token], bh.config.Parallel)
	processorWorkers := make([]pipeline.Worker[*models.Token], bh.config.Parallel)

	partitionRanges, err := splitPartitions(
		bh.config.Partitions.Begin,
		bh.config.Partitions.Count,
		bh.config.Parallel,
	)
	if err != nil {
		return err
	}

	scanPolicy := *bh.config.ScanPolicy

	// if we are using the asb encoder, we need to set the RawCDT flag
	// in the scan policy so that maps and lists are returned as raw blob bins
	if _, ok := bh.config.EncoderFactory.(*encoding.ASBEncoderFactory); ok {
		scanPolicy.RawCDT = true
	}

	for i := 0; i < bh.config.Parallel; i++ {
		ARRCFG := arrConfig{
			Namespace:      bh.namespace,
			Set:            bh.config.Set,
			FirstPartition: partitionRanges[i].Begin,
			NumPartitions:  partitionRanges[i].Count,
		}

		recordReader := newAerospikeRecordReader(
			bh.aerospikeClient,
			ARRCFG,
			&scanPolicy,
		)

		readWorkers[i] = newReadWorker(recordReader)

		ttlSetter := newProcessorTTL()
		processorWorkers[i] = newProcessorWorker(ttlSetter)
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

	return bh.worker.DoJob(ctx, job)
}

// **** Backup To Writer Handler ****

// BackupStats stores the status of a backup job
type BackupStats struct{}

// BackupHandler handles a backup job to a set of io.writers
type BackupHandler struct {
	stats  BackupStats
	config *BackupConfig
	errors chan error
	backupHandlerBase
	writers []io.Writer
}

// newBackupHandler creates a new BackupHandler
func newBackupHandler(config *BackupConfig, ac *a.Client, writers []io.Writer) *BackupHandler {
	namespace := config.Namespace
	backupHandler := newBackupHandlerBase(config, ac, namespace)

	return &BackupHandler{
		config:            config,
		writers:           writers,
		backupHandlerBase: *backupHandler,
	}
}

// run runs the backup job
// currently this should only be run once
func (bwh *BackupHandler) run(ctx context.Context) {
	bwh.errors = make(chan error, 1)

	go func(errChan chan<- error) {
		// NOTE: order is important here
		// if we close the errChan before we handle the panic
		// the panic will attempt to send on a closed channel
		defer close(errChan)
		defer handlePanic(errChan)

		batchSize := bwh.config.Parallel
		dataWriters := []*writeWorker[*models.Token]{}

		for i, writer := range bwh.writers {
			dw, err := getDataWriter(bwh.config.EncoderFactory, writer, bwh.namespace, i == 0)
			if err != nil {
				errChan <- err
				return
			}

			dataWriters = append(dataWriters, dw)
			// if we have not reached the batch size and we have more writers
			// continue to the next writer
			// if we are at the end of writers then run no matter what
			if i < len(bwh.writers)-1 && len(dataWriters) < batchSize {
				continue
			}

			err = bwh.backupHandlerBase.run(ctx, dataWriters)
			if err != nil {
				errChan <- err
				return
			}

			clear(dataWriters)
		}
	}(bwh.errors)
}

// GetStats returns the stats of the backup job
func (bwh *BackupHandler) GetStats() BackupStats {
	return bwh.stats
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

// **** Backup To Directory Handler ****

type BackupToDirectoryStats struct {
	BackupStats
}

// BackupToDirectoryHandler handles a backup job to a directory
type BackupToDirectoryHandler struct {
	stats           BackupToDirectoryStats
	config          *BackupToDirectoryConfig
	aerospikeClient *a.Client
	errors          chan error
	directory       string
}

// newBackupToDirectoryHandler creates a new BackupToDirectoryHandler
func newBackupToDirectoryHandler(config *BackupToDirectoryConfig, ac *a.Client, directory string) *BackupToDirectoryHandler {
	return &BackupToDirectoryHandler{
		config:          config,
		aerospikeClient: ac,
		directory:       directory,
	}
}

// run runs the backup job
// currently this should only be run once
func (bwh *BackupToDirectoryHandler) run(ctx context.Context) {
	bwh.errors = make(chan error, 1)

	go func(errChan chan<- error) {
		// NOTE: order is important here
		// if we close the errChan before we handle the panic
		// the panic will attempt to send on a closed channel
		defer close(errChan)
		defer handlePanic(errChan)

		err := prepareBackupDirectory(bwh.directory)
		if err != nil {
			errChan <- err
			return
		}

		writers := make([]io.Writer, bwh.config.Parallel)

		for i := range bwh.config.Parallel {
			fileName := getBackupFileName(bwh.config.Namespace, i, bwh.config.EncoderFactory)
			filePath := filepath.Join(bwh.directory, fileName)

			file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0666)
			if err != nil {
				err = fmt.Errorf("failed to create backup file %s: %w", filePath, err)
				errChan <- err
				return
			}

			defer file.Close()

			// buffer writes for efficiency
			bufferedFile := bufio.NewWriter(file)
			defer bufferedFile.Flush()

			writers[i] = bufferedFile
		}

		handler := newBackupHandler(&bwh.config.BackupConfig, bwh.aerospikeClient, writers)
		handler.run(ctx)

		err = handler.Wait(ctx)
		if err != nil {
			errChan <- err
			return
		}
	}(bwh.errors)
}

// GetStats returns the stats of the backup job
func (bwh *BackupToDirectoryHandler) GetStats() BackupToDirectoryStats {
	return bwh.stats
}

// Wait waits for the backup job to complete and returns an error if the job failed
func (bwh *BackupToDirectoryHandler) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-bwh.errors:
		return err
	}
}

// **** Backup To Directory IO Writer ****

// BackupToDirectoryWriter is an io.Writer that writes to a directory
// when it's size limit is reached, it will create a new file and continue writing

// **** Helper Functions ****

func getDataWriter(eb EncoderFactory, w io.Writer, namespace string, first bool) (*writeWorker[*models.Token], error) {
	enc, err := eb.CreateEncoder(w)
	if err != nil {
		return nil, err
	}

	switch encT := enc.(type) {
	case *asb.Encoder:
		asbw := newAsbWriter(encT)

		err := asbw.Init(namespace, first)
		if err != nil {
			return nil, err
		}

		worker := newWriteWorker(asbw)

		return worker, err

	default:
		gw := newGenericWriter(encT)
		worker := newWriteWorker(gw)

		return worker, nil
	}
}

var ErrBackupDirectoryInvalid = errors.New("backup directory is invalid")

func prepareBackupDirectory(dir string) error {
	DirInfo, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(dir, 0755)
			if err != nil {
				return fmt.Errorf("%w: failed to create backup directory %s: %v", ErrBackupDirectoryInvalid, dir, err)
			}
		}
	} else if !DirInfo.IsDir() {
		return fmt.Errorf("%w: %s is not a directory", ErrBackupDirectoryInvalid, dir)
	}

	fileInfo, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("%w: failed to read %s: %w", ErrBackupDirectoryInvalid, dir, err)
	}

	if len(fileInfo) > 0 {
		return fmt.Errorf("%w: %s is not empty", ErrBackupDirectoryInvalid, dir)
	}

	return nil
}

func getBackupFileName(namespace string, id int, encoder EncoderFactory) string {
	name := fmt.Sprintf("%s_%d", namespace, id)

	switch encoder.(type) {
	case *encoding.ASBEncoderFactory:
		name += ".asb"
	}

	return name
}
