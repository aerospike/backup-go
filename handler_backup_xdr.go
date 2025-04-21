// Copyright 2024 Aerospike, Inc.
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
	"fmt"
	"log/slog"
	"sync"

	"github.com/aerospike/backup-go/internal/asinfo"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/internal/processors"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"github.com/google/uuid"
)

// HandlerBackupXDR handles a backup job over XDR protocol.
type HandlerBackupXDR struct {
	id string

	ctx    context.Context
	cancel context.CancelFunc

	readProcessor   *recordReaderProcessor[*models.ASBXToken]
	writerProcessor *fileWriterProcessor[*models.ASBXToken]
	encoder         Encoder[*models.ASBXToken]
	config          *ConfigBackupXDR
	infoClient      *asinfo.InfoClient
	stats           *models.BackupStats

	logger *slog.Logger

	errors chan error
	// For graceful shutdown.
	wg sync.WaitGroup

	pl *pipeline.Pipeline[*models.ASBXToken]

	rpsCollector *metrics.RPSCollector
}

// newHandlerBackupXDR returns a new xdr backup handler.
func newBackupXDRHandler(
	ctx context.Context,
	config *ConfigBackupXDR,
	aerospikeClient AerospikeClient,
	writer Writer,
	logger *slog.Logger,
) *HandlerBackupXDR {
	id := uuid.NewString()
	logger = logging.WithHandler(logger, id, logging.HandlerTypeBackup, writer.GetType())

	// redefine context cancel.
	ctx, cancel := context.WithCancel(ctx)

	encoder := NewEncoder[*models.ASBXToken](config.EncoderType, config.Namespace, false)

	stats := models.NewBackupStats()

	infoClient := asinfo.NewInfoClientFromAerospike(aerospikeClient, config.InfoPolicy, config.InfoRetryPolicy)

	rpsCollector := metrics.NewRPSCollector(ctx, logger)

	readProcessor := newRecordReaderProcessor[*models.ASBXToken](
		config,
		aerospikeClient,
		infoClient,
		nil,
		nil,
		rpsCollector,
		logger,
	)

	writerProcessor := newFileWriterProcessor[*models.ASBXToken](
		emptyPrefixSuffix,
		emptyPrefixSuffix,
		nil,
		writer,
		encoder,
		config.EncryptionPolicy,
		config.SecretAgentConfig,
		config.CompressionPolicy,
		nil,
		stats,
		nil,
		config.FileLimit,
		config.ParallelWrite,
		logger,
	)

	return &HandlerBackupXDR{
		id:              id,
		ctx:             ctx,
		cancel:          cancel,
		encoder:         encoder,
		readProcessor:   readProcessor,
		writerProcessor: writerProcessor,
		config:          config,
		infoClient:      infoClient,
		stats:           stats,
		logger:          logger,
		errors:          make(chan error, 1),
		rpsCollector:    rpsCollector,
	}
}

// run runs the backup job.
// currently this should only be run once.
func (bh *HandlerBackupXDR) run() {
	bh.wg.Add(1)
	bh.stats.Start()

	go doWork(bh.errors, bh.logger, func() error {
		defer bh.wg.Done()
		return bh.backup(bh.ctx)
	})
}

func (bh *HandlerBackupXDR) backup(ctx context.Context) error {
	var err error

	// Count total records.
	bh.stats.TotalRecords, err = bh.infoClient.GetRecordCount(bh.config.Namespace, nil)
	if err != nil {
		return fmt.Errorf("failed to get records count: %w", err)
	}

	// Read workers.
	readWorkers, err := bh.readProcessor.newReadWorkersXDR(ctx)
	if err != nil {
		return fmt.Errorf("failed create read workers: %w", err)
	}

	// Write workers.
	backupWriters, err := bh.writerProcessor.newWriters(ctx)
	if err != nil {
		return fmt.Errorf("failed to create storage writers: %w", err)
	}

	defer closeWriters(backupWriters, bh.logger)

	writeWorkers := bh.writerProcessor.newWriteWorkers(backupWriters)

	// Process workers.
	composeProcessor := newTokenWorker[*models.ASBXToken](
		processors.NewTokenCounter[*models.ASBXToken](&bh.stats.ReadRecords),
		1)

	// Create a pipeline and start.
	pl, err := pipeline.NewPipeline(
		pipeline.ModeSingleParallel, bh.splitFunc,
		readWorkers,
		composeProcessor,
		writeWorkers,
	)
	if err != nil {
		return fmt.Errorf("failed to create pipeline: %w", err)
	}

	// Assign, so we can get pl stats.
	bh.pl = pl

	return pl.Run(ctx)
}

// Wait waits for the backup job to complete and returns an error if the job failed.
func (bh *HandlerBackupXDR) Wait(ctx context.Context) error {
	defer func() {
		bh.stats.Stop()
	}()

	select {
	case <-bh.ctx.Done():
		// When global context is done, wait until all routines finish their work properly.
		// Global context - is context that was passed to Backup() method.
		bh.wg.Wait()

		return bh.ctx.Err()
	case <-ctx.Done():
		// When local context is done, we cancel global context.
		// Then wait until all routines finish their work properly.
		// Local context - is context that was passed to Wait() method.
		bh.cancel()
		bh.wg.Wait()

		return ctx.Err()
	case err := <-bh.errors:
		return err
	}
}

// splitFunc distributes token between pipeline workers.
func (bh *HandlerBackupXDR) splitFunc(t *models.ASBXToken) int {
	partPerWorker := MaxPartitions / bh.config.ParallelWrite

	var id int
	if partPerWorker > 0 {
		id = t.Key.PartitionId() / partPerWorker
	}

	if id >= bh.config.ParallelWrite {
		return id - 1
	}

	return id
}

// GetStats returns the stats of the backup job.
func (bh *HandlerBackupXDR) GetStats() *models.BackupStats {
	return bh.stats
}

// GetMetrics returns the rpsCollector of the backup job.
func (bh *HandlerBackupXDR) GetMetrics() *models.Metrics {
	pr, pw := bh.pl.GetMetrics()
	return models.NewMetrics(pr, pw, bh.rpsCollector)
}
