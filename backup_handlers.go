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
	"context"
	"io"
	"log/slog"

	"github.com/aerospike/backup-go/encoding"
	"github.com/aerospike/backup-go/encoding/asb"
	"github.com/aerospike/backup-go/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"github.com/google/uuid"

	a "github.com/aerospike/aerospike-client-go/v7"
)

// **** Base Backup Handler ****

type backupHandlerBase struct {
	worker          workHandler
	config          *BackupConfig
	aerospikeClient *a.Client
	logger          *slog.Logger
	namespace       string
}

func newBackupHandlerBase(config *BackupConfig, ac *a.Client,
	namespace string, logger *slog.Logger) *backupHandlerBase {
	logger.Debug("created new backup base handler")

	wh := newWorkHandler()

	handler := &backupHandlerBase{
		namespace:       namespace,
		config:          config,
		aerospikeClient: ac,
		worker:          *wh,
		logger:          logger,
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
			bh.logger,
		)

		readWorkers[i] = newReadWorker(recordReader)

		voidTimeSetter := newProcessorVoidTime(bh.logger)
		processorWorkers[i] = newProcessorWorker(voidTimeSetter)
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
// the stats are updated in realtime by backup jobs
type BackupStats struct {
	tokenStats
}

// BackupHandler handles a backup job to a set of io.writers
type BackupHandler struct {
	config *BackupConfig
	errors chan error
	logger *slog.Logger
	backupHandlerBase
	id      string
	writers []io.Writer
	stats   BackupStats
}

// newBackupHandler creates a new BackupHandler
func newBackupHandler(config *BackupConfig, ac *a.Client, writers []io.Writer, logger *slog.Logger) *BackupHandler {
	id := uuid.NewString()
	logger = logging.WithHandler(logger, id, logging.HandlerTypeBackup)

	namespace := config.Namespace
	backupHandler := newBackupHandlerBase(config, ac, namespace, logger)

	return &BackupHandler{
		config:            config,
		writers:           writers,
		backupHandlerBase: *backupHandler,
		logger:            logger,
		id:                id,
	}
}

// run runs the backup job
// currently this should only be run once
func (bwh *BackupHandler) run(ctx context.Context) {
	bwh.errors = make(chan error, 1)

	go doWork(bwh.errors, bwh.logger, func() error {
		batchSize := bwh.config.Parallel
		writeWorkers := []*writeWorker[*models.Token]{}

		for i, writer := range bwh.writers {
			encoder, err := bwh.config.EncoderFactory.CreateEncoder()
			if err != nil {
				return err
			}

			// asb files require a header, treat the
			// passed in io.Writer like a fresh file and write the header
			if _, ok := encoder.(*asb.Encoder); ok {
				err := writeASBHeader(writer, bwh.config.Namespace, i == 0)
				if err != nil {
					return err
				}
			}

			var dataWriter dataWriter[*models.Token] = newTokenWriter(encoder, writer, bwh.logger)
			dataWriter = newWriterWithTokenStats(dataWriter, &bwh.stats, bwh.logger)
			worker := newWriteWorker(dataWriter)
			writeWorkers = append(writeWorkers, worker)
			// if we have not reached the batch size and we have more writers
			// continue to the next writer
			// if we are at the end of writers then run no matter what
			if i < len(bwh.writers)-1 && len(writeWorkers) < batchSize {
				continue
			}

			err = bwh.backupHandlerBase.run(ctx, writeWorkers)
			if err != nil {
				return err
			}

			writeWorkers = []*writeWorker[*models.Token]{}
		}

		return nil
	})
}

// GetStats returns the stats of the backup job
func (bwh *BackupHandler) GetStats() *BackupStats {
	return &bwh.stats
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
