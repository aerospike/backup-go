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
	"sync/atomic"

	"github.com/aerospike/backup-go/internal/bandwidth"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/internal/processors"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe"
	"github.com/aerospike/backup-go/pkg/estimates"
	"github.com/google/uuid"
)

// StreamingReader defines an interface for accessing backup file data from a storage provider.
// Implementations, handling different storage types, are located within the io.storage package.
type StreamingReader interface {
	// StreamFiles creates readers from files and sends them to the channel.
	// In case of an error, the error is sent to the error channel.
	// Must be run in a goroutine `go rh.reader.StreamFiles(ctx, readersCh, errorsCh, skipPrefixes)`.
	StreamFiles(context.Context, chan<- models.File, chan<- error, []string)

	// StreamFile creates a single file reader and sends io.Readers to the `readersCh`
	// In case of an error, it is sent to the `errorsCh` channel.
	// Must be run in a goroutine `go rh.reader.StreamFile()`.
	StreamFile(ctx context.Context, filename string, readersCh chan<- models.File, errorsCh chan<- error)

	// GetType returns the type of storage. Used in logging.
	GetType() string

	// ListObjects return list of objects in the path.
	ListObjects(ctx context.Context, path string) ([]string, error)

	// GetSize returns the size of asb files in the path.
	GetSize() int64

	// GetNumber returns the number of asb files in the path.
	GetNumber() int64

	// GetSkipped returns a list of file paths that were skipped during the `StreamFlies` with skipPrefix.
	GetSkipped() []string
}

// restoreHandler handles a restore job using the given reader.
type restoreHandler struct {
	*handlerBase

	readProcessor  *fileReaderProcessor
	writeProcessor *recordWriterProcessor
	config         *ConfigRestore
	stats          *models.RestoreStats

	logger  *slog.Logger
	limiter *bandwidth.Limiter

	pl            atomic.Pointer[pipe.Pipe]
	rpsCollector  *metrics.Collector
	kbpsCollector *metrics.Collector

	id string
}

// newRestoreHandler creates a new restoreHandler.
func newRestoreHandler(
	ctx context.Context,
	config *ConfigRestore,
	aerospikeClient AerospikeClient,
	logger *slog.Logger,
	reader StreamingReader,
	infoClient ClusterInfo,
) (*restoreHandler, error) {
	id := uuid.NewString()[:6]
	logger = logging.WithHandler(logger, id, logging.HandlerTypeRestore, reader.GetType())
	metricMessage := fmt.Sprintf("%s metrics %s", logging.HandlerTypeRestore, id)

	// Create handler base first to get the derived context.
	base := newHandlerBase(ctx)

	// Channel for transferring readers.
	readersCh := make(chan models.File)

	stats := models.NewRestoreStats()

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

	encryptionKey, err := resolveEncryptionKey(base.ctx, config.EncryptionPolicy, config.SecretAgentConfig)
	if err != nil {
		base.cancel()
		return nil, err
	}

	readProcessor := newFileReaderProcessor(
		reader,
		config,
		encryptionKey,
		kbpsCollector,
		readersCh,
		base.errors,
		logger,
	)

	writeProcessor := newRecordWriterProcessor(
		aerospikeClient,
		config,
		stats,
		rpsCollector,
		infoClient,
		logger,
	)

	limiter, err := bandwidth.NewLimiter(config.Bandwidth)
	if err != nil {
		base.cancel()
		return nil, fmt.Errorf("failed to create bandwidth limiter: %w", err)
	}

	return &restoreHandler{
		handlerBase:    base,
		readProcessor:  readProcessor,
		writeProcessor: writeProcessor,
		config:         config,
		stats:          stats,
		id:             id,
		logger:         logger,
		limiter:        limiter,
		rpsCollector:   rpsCollector,
		kbpsCollector:  kbpsCollector,
	}, nil
}

func (rh *restoreHandler) run() {
	rh.stats.Start()

	go estimates.PrintFilesNumber(rh.ctx, rh.readProcessor.reader.GetNumber, rh.logger)
	go estimates.PrintRestoreEstimate(rh.ctx, rh.stats, rh.GetMetrics, rh.readProcessor.reader.GetSize, rh.logger)

	rh.wg.Go(func() {
		doWork(rh.errors, rh.done, rh.logger, func() error {
			return rh.restore(rh.ctx)
		})
	})
}

func (rh *restoreHandler) restore(ctx context.Context) error {
	dataReaders := rh.readProcessor.newDataReaders(ctx)

	if err := rh.runPipeline(ctx, dataReaders); err != nil {
		return err
	}

	// Apply metadata at the end.
	if rh.config.ApplyMetadataLast {
		if err := rh.restoreMetadata(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (rh *restoreHandler) restoreMetadata(ctx context.Context) error {
	metadataReaders := rh.readProcessor.newMetadataReaders(ctx)

	if len(metadataReaders) == 0 {
		rh.logger.Debug("metadata readers not found")

		return nil
	}

	if err := rh.runPipeline(ctx, metadataReaders); err != nil {
		return fmt.Errorf("failed to apply metadata: %w", err)
	}

	rh.logger.Info("metadata applied after records")

	return nil
}

func (rh *restoreHandler) runPipeline(ctx context.Context, dataReaders []pipe.Reader) error {
	dataWriters, err := rh.writeProcessor.newDataWriters(ctx)
	if err != nil {
		return fmt.Errorf("failed to create writer workers: %w", err)
	}

	composeProcessor := rh.getComposeProcessor(ctx)

	pl, err := pipe.NewPipe(
		composeProcessor,
		dataReaders,
		dataWriters,
		rh.limiter,
		pipe.RoundRobin,
	)
	if err != nil {
		return err
	}

	// Assign, so we can get pl stats.
	rh.pl.Store(pl)

	return pl.Run(ctx)
}

func (rh *restoreHandler) getComposeProcessor(ctx context.Context) pipe.ProcessorCreator {
	// Namespace Source and Destination
	var nsSource, nsDest *string
	if rh.config.Namespace != nil {
		nsSource = rh.config.Namespace.Source
		nsDest = rh.config.Namespace.Destination
	}

	return newDataProcessor(
		processors.NewRecordCounter(&rh.stats.ReadRecords),
		processors.NewSizeCounter(&rh.stats.TotalBytesRead),
		processors.NewFilterByType(
			rh.config.NoRecords,
			rh.config.NoIndexes,
			rh.config.NoUDFs,
			&rh.stats.RecordsSkipped,
		),
		processors.NewFilterBySet(rh.config.SetList, &rh.stats.RecordsSkipped),
		processors.NewFilterByBin(rh.config.BinList, &rh.stats.RecordsSkipped),
		processors.NewChangeNamespace(nsSource, nsDest),
		processors.NewExpirationSetter(&rh.stats.RecordsExpired, rh.config.ExtraTTL, rh.logger),
		processors.NewTPSLimiter(ctx, rh.config.RecordsPerSecond),
	)
}

// GetStats returns the stats of the restore job.
func (rh *restoreHandler) GetStats() *models.RestoreStats {
	return rh.stats
}

// Wait waits for the restore job to complete and returns an error if the job failed.
func (rh *restoreHandler) Wait(ctx context.Context) error {
	err := rh.waitForCompletion(ctx)

	rh.cleanup() // clean up resources.

	return err
}

// GetMetrics returns the metrics of the restore job.
func (rh *restoreHandler) GetMetrics() *models.Metrics {
	if rh == nil {
		return nil
	}

	var pr, pw int

	pl := rh.pl.Load()
	if pl != nil {
		pr, pw = pl.GetMetrics()
	}

	return models.NewMetrics(
		pr, pw,
		rh.rpsCollector.GetLastResult(),
		rh.kbpsCollector.GetLastResult(),
	)
}

// cleanup stops the collection of stats and metrics for the restore job,
// including RestoreStats, RPS, and KBPS tracking.
func (rh *restoreHandler) cleanup() {
	rh.stats.Stop()
	rh.rpsCollector.Stop()
	rh.kbpsCollector.Stop()

	pl := rh.pl.Load()
	if pl != nil {
		pl.Close()
	}

	rh.pl.Swap(nil)
}

var _ RestoreHandler = (*restoreHandler)(nil)
