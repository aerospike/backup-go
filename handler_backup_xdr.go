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

	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/internal/processors"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe"
	"github.com/google/uuid"
)

// HandlerBackupXDR handles a backup job over XDR protocol.
type HandlerBackupXDR struct {
	*handlerBase

	id string

	readerProcessor *recordReaderProcessorXDR[*models.ASBXToken]
	writerProcessor *fileWriterProcessor[*models.ASBXToken]
	encoder         Encoder[*models.ASBXToken]
	config          *ConfigBackupXDR
	infoClient      InfoGetter
	stats           *models.BackupStats

	logger *slog.Logger

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
	infoClient InfoGetter,
) (*HandlerBackupXDR, error) {
	id := uuid.NewString()
	logger = logging.WithHandler(logger, id, logging.HandlerTypeBackup, writer.GetType())
	metricMessage := fmt.Sprintf("%s metrics %s", logging.HandlerTypeBackup, id)

	// Create handler base first to get the derived context.
	base := newHandlerBase(ctx)

	hasExprSind, err := infoClient.HasExpressionSIndex(base.ctx, config.Namespace)
	if err != nil {
		base.cancel()
		return nil, fmt.Errorf("failed to check if expression sindex exists: %w", err)
	}

	encoder := NewEncoder[*models.ASBXToken](config.EncoderType, config.Namespace, false, hasExprSind)

	stats := models.NewBackupStats()

	rpsCollector := metrics.NewCollector(
		base.ctx,
		logger,
		metrics.RecordsPerSecond,
		metricMessage,
		config.MetricsEnabled,
	)

	kbpsCollector := metrics.NewCollector(
		base.ctx,
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

	writerProcessor, err := newFileWriterProcessor[*models.ASBXToken](
		"",
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
	if err != nil {
		base.cancel()
		return nil, err
	}

	return &HandlerBackupXDR{
		handlerBase:     base,
		id:              id,
		encoder:         encoder,
		readerProcessor: readerProcessor,
		writerProcessor: writerProcessor,
		config:          config,
		infoClient:      infoClient,
		stats:           stats,
		logger:          logger,
		rpsCollector:    rpsCollector,
		kbpsCollector:   kbpsCollector,
	}, nil
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

// backup starts the backup operation. It blocks until the backup is completed.
func (bh *HandlerBackupXDR) backup(ctx context.Context) error {
	// Count total records.
	records, err := bh.infoClient.GetRecordCount(ctx, bh.config.Namespace, nil)
	if err != nil {
		return fmt.Errorf("failed to get records count: %w", err)
	}

	bh.stats.TotalRecords.Store(records)

	// Create the data readers.
	readWorkers, err := bh.readerProcessor.newReadWorkersXDR(ctx)
	if err != nil {
		return fmt.Errorf("failed create read workers: %w", err)
	}

	// Create the data writers.
	_, writeWorkers, err := bh.writerProcessor.newDataWriters(ctx)
	if err != nil {
		return fmt.Errorf("failed create write workers: %w", err)
	}

	// Create the data processor.
	proc := newDataProcessor(
		processors.NewTokenCounter[*models.ASBXToken](&bh.stats.ReadRecords),
	)

	// Create and run the pipeline.
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
	err := bh.waitForCompletion(ctx)

	bh.cleanup() // clean up resources.

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
