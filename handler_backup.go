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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/aerospike/backup-go/internal/bandwidth"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/internal/processors"
	"github.com/aerospike/backup-go/io/aerospike"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe"
	"github.com/aerospike/backup-go/pkg/asinfo"
	"github.com/google/uuid"
	"golang.org/x/sync/semaphore"
)

// Writer defines an interface for writing backup data to a storage provider.
// Implementations, handling different storage types, are located within the io.storage package.
type Writer interface {
	// NewWriter returns new writer for backup logic to use. Each call creates
	// a new writer, they might be working in parallel. Backup logic will close
	// the writer after backup is done. Header func is executed on a writer
	// after creation (on each one in case of multipart file).
	NewWriter(ctx context.Context, filename string) (io.WriteCloser, error)

	// GetType returns the type of storage. Used in logging.
	GetType() string

	// RemoveFiles removes a backup file or files from directory.
	RemoveFiles(ctx context.Context) error

	// Remove removes a file or directory at the specified path from the backup storage.
	// Returns an error if the operation fails.
	Remove(ctx context.Context, path string) error
}

// BackupHandler handles a backup job.
type BackupHandler struct {
	// Global backup context for a whole backup process.
	ctx    context.Context
	cancel context.CancelFunc

	readerProcessor *recordReaderProcessor[*models.Token]
	writerProcessor *fileWriterProcessor[*models.Token]
	encoder         Encoder[*models.Token]
	config          *ConfigBackup
	aerospikeClient AerospikeClient
	recordCounter   *recordCounter

	logger                 *slog.Logger
	firstFileHeaderWritten *atomic.Bool
	limiter                *bandwidth.Limiter
	infoClient             *asinfo.InfoClient
	scanLimiter            *semaphore.Weighted
	errors                 chan error
	done                   chan struct{}
	id                     string

	stats *models.BackupStats
	// Backup state for continuation.
	state *State
	// For graceful shutdown.
	wg sync.WaitGroup

	pl atomic.Pointer[pipe.Pipe[*models.Token]]

	// records per second collector.
	rpsCollector *metrics.Collector
	// kilobytes per second collector.
	kbpsCollector *metrics.Collector
}

// newBackupHandler creates a new BackupHandler.
func newBackupHandler(
	ctx context.Context,
	config *ConfigBackup,
	ac AerospikeClient,
	logger *slog.Logger,
	writer Writer,
	reader StreamingReader,
	scanLimiter *semaphore.Weighted,
) (*BackupHandler, error) {
	id := uuid.NewString()
	// For estimates calculations, a writer will be nil.
	storageType := ""
	if writer != nil {
		storageType = writer.GetType()
	}

	logger = logging.WithHandler(logger, id, logging.HandlerTypeBackup, storageType)
	metricMessage := fmt.Sprintf("%s metrics %s", logging.HandlerTypeBackup, id)

	// redefine context cancel.
	ctx, cancel := context.WithCancel(ctx)

	var state *State

	if config.StateFile != "" {
		var err error

		state, err = NewState(ctx, config, reader, writer, logger)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to initialize state: %w", err)
		}

		// If it is a continuation operation, we load partition filters from state.
		if config.isStateContinue() {
			// change filters in config.
			config.PartitionFilters, err = state.loadPartitionFilters()
			if err != nil {
				cancel()
				return nil, fmt.Errorf("failed to load partition filters for : %w", err)
			}
		}
	}

	encoder := NewEncoder[*models.Token](config.EncoderType, config.Namespace, config.Compact)

	stats := models.NewBackupStats()

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

	infoCLient := asinfo.NewInfoClientFromAerospike(ac, config.InfoPolicy, config.InfoRetryPolicy)

	readerProcessor := newRecordReaderProcessor[*models.Token](
		config,
		ac,
		infoCLient,
		state,
		scanLimiter,
		rpsCollector,
		logger,
	)

	recCounter := newRecordCounter(ac, infoCLient, config, readerProcessor, logger)

	limiter, err := bandwidth.NewLimiter(config.Bandwidth)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create bandwidth limiter: %w", err)
	}

	bh := &BackupHandler{
		ctx:                    ctx,
		cancel:                 cancel,
		config:                 config,
		aerospikeClient:        ac,
		id:                     id,
		logger:                 logger,
		firstFileHeaderWritten: &atomic.Bool{},
		encoder:                encoder,
		readerProcessor:        readerProcessor,
		recordCounter:          recCounter,
		limiter:                limiter,
		infoClient:             infoCLient,
		scanLimiter:            scanLimiter,
		state:                  state,
		stats:                  stats,
		rpsCollector:           rpsCollector,
		kbpsCollector:          kbpsCollector,
		errors:                 make(chan error, 1),
		done:                   make(chan struct{}, 1),
	}

	writerProcessor := newFileWriterProcessor[*models.Token](
		emptyPrefixSuffix,
		bh.stateSuffixGenerator,
		writer,
		encoder,
		config.EncryptionPolicy,
		config.SecretAgentConfig,
		config.CompressionPolicy,
		state,
		stats,
		kbpsCollector,
		config.FileLimit,
		config.ParallelWrite,
		logger,
	)

	bh.writerProcessor = writerProcessor

	return bh, nil
}

// run runs the backup job.
// currently this should only be run once.
func (bh *BackupHandler) run() {
	bh.wg.Add(1)
	bh.stats.Start()

	go doWork(bh.errors, bh.done, bh.logger, func() error {
		defer bh.wg.Done()

		return bh.backup(bh.ctx)
	})
}

// getEstimate calculates backup size estimate.
func (bh *BackupHandler) getEstimate(ctx context.Context, recordsNumber int64) (uint64, error) {
	totalCount, err := bh.infoClient.GetRecordCount(bh.config.Namespace, bh.config.SetList)
	if err != nil {
		return 0, fmt.Errorf("failed to count records: %w", err)
	}

	// Calculate headers size.
	header := bh.encoder.GetHeader(0)
	headerSize := len(header) * bh.config.ParallelWrite

	// Calculate records size.
	samples, samplesData, err := bh.getEstimateSamples(ctx, recordsNumber)
	if err != nil {
		return 0, fmt.Errorf("failed to estimate samples: %w", err)
	}

	// Calculate compress ratio. For uncompressed data it would be 1.
	compressRatio, err := getCompressRatio(bh.config.CompressionPolicy, samplesData)
	if err != nil {
		return 0, fmt.Errorf("failed to get compress ratio: %w", err)
	}

	bh.logger.Debug("compression", slog.Float64("ratio", compressRatio))

	result := getEstimate(samples, float64(totalCount), bh.logger)
	// Add headers.
	result += float64(headerSize)
	// Apply compression ratio. (For uncompressed it will be 1)
	result /= compressRatio

	return uint64(result), nil
}

// getEstimateSamples returns a slice of samples and its content for estimate calculations.
func (bh *BackupHandler) getEstimateSamples(ctx context.Context, recordsNumber int64,
) (samples []float64, samplesData []byte, err error) {
	scanPolicy := *bh.config.ScanPolicy
	scanPolicy.MaxRecords = recordsNumber
	// we need to set the RawCDT flag
	// in the scan policy so that maps and lists are returned as raw blob bins
	scanPolicy.RawCDT = true

	nodes := bh.aerospikeClient.GetNodes()
	readerConfig := bh.readerProcessor.recordReaderConfigForNode(nodes, &scanPolicy)
	recordReader := aerospike.NewRecordReader(ctx, bh.aerospikeClient, readerConfig, bh.logger)

	// Timestamp processor.
	tsProcessor := processors.NewVoidTimeSetter[*models.Token](bh.logger)

	for {
		t, err := recordReader.Read(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, nil, fmt.Errorf("failed to read records: %w", err)
		}

		t, err = tsProcessor.Process(t)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to process token: %w", err)
		}

		data, err := bh.encoder.EncodeToken(t)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to encode token: %w", err)
		}

		samples = append(samples, float64(len(data)))
		samplesData = append(samplesData, data...)
	}

	return samples, samplesData, nil
}

func (bh *BackupHandler) backup(ctx context.Context) error {
	backupWriters, err := bh.writerProcessor.newWriters(ctx)
	if err != nil {
		return err
	}

	dataWriters := bh.writerProcessor.newDataWriters(backupWriters)

	defer closeWriters(backupWriters, bh.logger)

	// backup secondary indexes and UDFs on the first writer
	// this is done to match the behavior of the
	// backup c tool and keep the backup files more consistent
	// at some point we may want to treat the secondary indexes/UDFs
	// like records and back them up as part of the same pipeline
	// but doing so would cause them to be mixed in with records in the backup file(s)
	err = bh.backupSIndexesAndUDFs(ctx, backupWriters[0])
	if err != nil {
		return err
	}

	if bh.config.NoRecords {
		// no need to run backup handler
		return nil
	}

	// start counting backup records in a separate goroutine to estimate the total number of records.
	// This is done in parallel with the backup process to avoid delaying the start of the backup.
	// The estimated backup record count will be available in statistics once the estimation process is completed.
	go bh.countRecords(ctx)

	if bh.config.isStateContinue() {
		// Have to reload filter, as on count records cursor is moving and future scans returns nothing.
		bh.config.PartitionFilters, err = bh.state.loadPartitionFilters()
		if err != nil {
			return err
		}
	}

	// Calculate the Records Per Second (RPS) target for each individual parallel reader/processor.
	// This value evenly distributes the overall read rate across the parallel workers,
	// ensuring each worker adheres to a portion of the total RPS limit.
	rps := bh.config.RecordsPerSecond / bh.config.ParallelRead

	proc := newDataProcessor(
		processors.NewRecordCounter[*models.Token](&bh.stats.ReadRecords),
		processors.NewVoidTimeSetter[*models.Token](bh.logger),
		processors.NewTPSLimiter[*models.Token](
			ctx, rps),
	)

	dataReaders, err := bh.readerProcessor.newAerospikeReadWorkers(ctx, bh.config.ParallelRead)
	if err != nil {
		return err
	}

	pipelineMode := pipe.RoundRobin
	if bh.config.StateFile != "" || len(dataReaders) == len(dataWriters) {
		pipelineMode = pipe.Fixed
	}

	pl, err := pipe.NewPipe(
		proc,
		dataReaders,
		dataWriters,
		bh.limiter,
		pipelineMode,
	)
	if err != nil {
		return err
	}

	// Assign, so we can get pl metrics.
	bh.pl.Store(pl)

	return pl.Run(ctx)
}

func (bh *BackupHandler) countRecords(ctx context.Context) {
	records, err := bh.recordCounter.countRecords(ctx, bh.infoClient)
	if err != nil {
		bh.logger.Error("failed to count records", slog.Any("error", err))
		return
	}

	bh.stats.TotalRecords.Store(records)
}

func closeWriters(backupWriters []io.WriteCloser, logger *slog.Logger) {
	for _, w := range backupWriters {
		if err := w.Close(); err != nil {
			logger.Error("failed to close backup file", "error", err)
		}
	}
}

func (bh *BackupHandler) backupSIndexesAndUDFs(
	ctx context.Context,
	writer io.WriteCloser,
) error {
	if !bh.config.NoIndexes {
		err := bh.backupSIndexes(ctx, writer)
		if err != nil {
			return fmt.Errorf("failed to backup secondary indexes: %w", err)
		}
	}

	if !bh.config.NoUDFs {
		err := bh.backupUDFs(ctx, writer)
		if err != nil {
			return fmt.Errorf("failed to backup UDFs: %w", err)
		}
	}

	return nil
}

// GetStats returns the stats of the backup job.
func (bh *BackupHandler) GetStats() *models.BackupStats {
	return bh.stats
}

// Wait waits for the backup job to complete and returns an error if the job failed.
func (bh *BackupHandler) Wait(ctx context.Context) error {
	var err error

	select {
	case <-bh.ctx.Done():
		// When global context is done, wait until all routine finish their work properly.
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

	// If the err is nil, we can remove the state file.
	if err == nil && bh.state != nil {
		// Clen only if err == nil and state is not nil.
		if err = bh.state.cleanup(ctx); err != nil {
			bh.logger.Error("failed to cleanup state", slog.Any("error", err))
		}
	}

	// Clean.
	bh.cleanup()

	return err
}

func (bh *BackupHandler) backupSIndexes(
	ctx context.Context,
	writer io.Writer,
) error {
	dataReader := aerospike.NewSIndexReader(bh.infoClient, bh.config.Namespace, bh.logger)

	var stInfo *stateInfo
	if bh.state != nil {
		stInfo = newStateInfo(bh.state.RecordsStateChan, -1)
	}

	sindexWriter := pipe.Writer[*models.Token](
		newTokenWriter(
			bh.encoder,
			writer,
			bh.logger,
			stInfo,
		),
	)

	sindexWriter = newWriterWithTokenStats(sindexWriter, bh.stats, bh.logger)

	proc := newDataProcessor(processors.NewNoop[*models.Token]())

	sindexPipeline, err := pipe.NewPipe(
		proc,
		[]pipe.Reader[*models.Token]{dataReader},
		[]pipe.Writer[*models.Token]{sindexWriter},
		bh.limiter,
		pipe.Fixed,
	)
	if err != nil {
		return err
	}

	return sindexPipeline.Run(ctx)
}

func (bh *BackupHandler) backupUDFs(
	ctx context.Context,
	writer io.Writer,
) error {
	dataReader := aerospike.NewUDFReader(bh.infoClient, bh.logger)

	var stInfo *stateInfo
	if bh.state != nil {
		stInfo = newStateInfo(bh.state.RecordsStateChan, -1)
	}

	udfWriter := pipe.Writer[*models.Token](
		newTokenWriter(
			bh.encoder,
			writer,
			bh.logger,
			stInfo,
		),
	)

	udfWriter = newWriterWithTokenStats(udfWriter, bh.stats, bh.logger)

	proc := newDataProcessor(processors.NewNoop[*models.Token]())

	udfPipeline, err := pipe.NewPipe(
		proc,
		[]pipe.Reader[*models.Token]{dataReader},
		[]pipe.Writer[*models.Token]{udfWriter},
		bh.limiter,
		pipe.Fixed,
	)
	if err != nil {
		return err
	}

	return udfPipeline.Run(ctx)
}

// GetMetrics returns metrics of the backup job.
func (bh *BackupHandler) GetMetrics() *models.Metrics {
	if bh == nil {
		return nil
	}

	var pr, pw int

	pl := bh.pl.Load()
	if pl != nil {
		pr, pw = pl.GetMetrics()
	}

	return models.NewMetrics(
		pr, pw,
		bh.rpsCollector.GetLastResult(),
		bh.kbpsCollector.GetLastResult(),
	)
}

// stateSuffixGenerator returns state suffix generator.
func (bh *BackupHandler) stateSuffixGenerator() string {
	suffix := ""
	if bh.state != nil {
		suffix = bh.state.getFileSuffix()
	}

	return suffix
}

// cleanup stops the collection of stats and metrics for the backup job,
// including BackupStats, RPS, and KBPS tracking.
func (bh *BackupHandler) cleanup() {
	bh.stats.Stop()
	bh.rpsCollector.Stop()
	bh.kbpsCollector.Stop()

	pl := bh.pl.Load()
	if pl != nil {
		pl.Close()
	}

	bh.pl.Swap(nil)
}
