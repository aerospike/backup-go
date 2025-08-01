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

	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/internal/processors"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe"
	"github.com/aerospike/backup-go/pkg/asinfo"
	"github.com/google/uuid"
)

// HandlerBackupXDR handles a backup job over XDR protocol.
type HandlerBackupXDR struct {
	id string

	ctx    context.Context
	cancel context.CancelFunc

	readerProcessor *recordReaderProcessorXDR[*models.ASBXToken]
	writerProcessor *fileWriterProcessor[*models.ASBXToken]
	encoder         Encoder[*models.ASBXToken]
	config          *ConfigBackupXDR
	infoClient      *asinfo.InfoClient
	stats           *models.BackupStats

	logger *slog.Logger

	errors chan error
	done   chan struct{}
	// For graceful shutdown.
	wg sync.WaitGroup

	pl *pipe.Pipe[*models.ASBXToken]

	// records per second collector.
	rpsCollector *metrics.Collector
	// kilobytes per second collector.
	kbpsCollector *metrics.Collector
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
	metricMessage := fmt.Sprintf("%s metrics %s", logging.HandlerTypeBackup, id)

	// redefine context cancel.
	ctx, cancel := context.WithCancel(ctx)

	encoder := NewEncoder[*models.ASBXToken](config.EncoderType, config.Namespace, false)

	stats := models.NewBackupStats()

	infoClient := asinfo.NewInfoClientFromAerospike(aerospikeClient, config.InfoPolicy, config.InfoRetryPolicy)

	rpsCollector := metrics.NewCollector(
		ctx,
		logger,
		metrics.RecordsPerSecond,
		metricMessage,
		config.MetricsEnabled,
	)

	kbpsCollector := metrics.NewCollector(
		ctx,
		logger,
		metrics.KilobytesPerSecond,
		metricMessage,
		config.MetricsEnabled,
	)

	readerProcessor := newRecordReaderProcessorXDR[*models.ASBXToken](
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
		writer,
		encoder,
		config.EncryptionPolicy,
		config.SecretAgentConfig,
		config.CompressionPolicy,
		nil,
		stats,
		kbpsCollector,
		config.FileLimit,
		config.ParallelWrite,
		logger,
	)

	return &HandlerBackupXDR{
		id:              id,
		ctx:             ctx,
		cancel:          cancel,
		encoder:         encoder,
		readerProcessor: readerProcessor,
		writerProcessor: writerProcessor,
		config:          config,
		infoClient:      infoClient,
		stats:           stats,
		logger:          logger,
		errors:          make(chan error, 1),
		done:            make(chan struct{}, 1),
		rpsCollector:    rpsCollector,
		kbpsCollector:   kbpsCollector,
	}
}

// run runs the backup job.
// currently this should only be run once.
func (bh *HandlerBackupXDR) run() {
	bh.wg.Add(1)
	bh.stats.Start()

	go doWork(bh.errors, bh.done, bh.logger, func() error {
		defer bh.wg.Done()

		return bh.backup(bh.ctx)
	})
}

func (bh *HandlerBackupXDR) backup(ctx context.Context) error {
	// Count total records.
	records, err := bh.infoClient.GetRecordCount(bh.config.Namespace, nil)
	if err != nil {
		return fmt.Errorf("failed to get records count: %w", err)
	}

	bh.stats.TotalRecords.Store(records)

	// Read workers.
	readWorkers, err := bh.readerProcessor.newReadWorkersXDR(ctx)
	if err != nil {
		return fmt.Errorf("failed create read workers: %w", err)
	}

	// Write workers.
	backupWriters, err := bh.writerProcessor.newWriters(ctx)
	if err != nil {
		return fmt.Errorf("failed to create storage writers: %w", err)
	}

	writeWorkers := bh.writerProcessor.newDataWriters(backupWriters)

	defer closeWriters(backupWriters, bh.logger)

	proc := newDataProcessor(
		processors.NewTokenCounter[*models.ASBXToken](&bh.stats.ReadRecords),
	)

	pl, err := pipe.NewPipe(
		proc,
		readWorkers,
		writeWorkers,
		nil,
		pipe.Split,
	)
	if err != nil {
		return err
	}

	// Assign, so we can get pl stats.
	bh.pl = pl

	return pl.Run(ctx)
}

// Wait waits for the backup job to complete and returns an error if the job failed.
func (bh *HandlerBackupXDR) Wait(ctx context.Context) error {
	var err error

	select {
	case <-bh.ctx.Done():
		// When global context is done, wait until all routines finish their work properly.
		// Global context - is context that was passed to Backup() method.
		err = bh.ctx.Err()
	case <-ctx.Done():
		// When local context is done, we cancel global context.
		// Then wait until all routines finish their work properly.
		// Local context - is context that was passed to Wait() method.
		bh.cancel()

		err = ctx.Err()
	case err = <-bh.errors:
		// On error, we cancel global context.
		// To stop all goroutines and prevent leaks.
		bh.cancel()
	case <-bh.done: // Success
	}

	// Wait when all routines ended.
	bh.wg.Wait()

	// Clean.
	bh.cleanup()

	return err
}

// GetStats returns the stats of the backup job.
func (bh *HandlerBackupXDR) GetStats() *models.BackupStats {
	return bh.stats
}

// GetMetrics returns metrics of the backup job.
func (bh *HandlerBackupXDR) GetMetrics() *models.Metrics {
	if bh == nil {
		return nil
	}

	var pr, pw int
	if bh.pl != nil {
		pr, pw = bh.pl.GetMetrics()
	}

	return models.NewMetrics(
		pr, pw,
		bh.rpsCollector.GetLastResult(),
		bh.kbpsCollector.GetLastResult(),
	)
}

// cleanup stops the collection of stats and metrics for the backup job,
// including BackupStats, RPS, and KBPS tracking.
func (bh *HandlerBackupXDR) cleanup() {
	bh.stats.Stop()
	bh.rpsCollector.Stop()
	bh.kbpsCollector.Stop()
}
