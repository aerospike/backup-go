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
	"sync/atomic"

	"github.com/aerospike/backup-go/internal/bandwidth"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/internal/processors"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe"
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

	// GetSize returns the size of asb/asbx files in the path.
	GetSize() int64

	// GetNumber returns the number of asb/asbx files in the path.
	GetNumber() int64

	// GetSkipped returns a list of file paths that were skipped during the `StreamFlies` with skipPrefix.
	GetSkipped() []string
}

// RestoreHandler handles a restore job using the given reader.
type RestoreHandler[T models.TokenConstraint] struct {
	// Global backup context for a whole restore process.
	ctx    context.Context
	cancel context.CancelFunc

	readProcessor  *fileReaderProcessor[T]
	writeProcessor *recordWriterProcessor[T]
	config         *ConfigRestore
	stats          *models.RestoreStats

	logger  *slog.Logger
	limiter *bandwidth.Limiter

	pl            atomic.Pointer[pipe.Pipe[T]]
	rpsCollector  *metrics.Collector
	kbpsCollector *metrics.Collector

	id     string
	errors chan error
	done   chan struct{}

	// For graceful shutdown.
	wg sync.WaitGroup
}

// newRestoreHandler creates a new RestoreHandler.
func newRestoreHandler[T models.TokenConstraint](
	ctx context.Context,
	config *ConfigRestore,
	aerospikeClient AerospikeClient,
	logger *slog.Logger,
	reader StreamingReader,
	infoClient InfoGetter,
) (*RestoreHandler[T], error) {
	id := uuid.NewString()
	logger = logging.WithHandler(logger, id, logging.HandlerTypeRestore, reader.GetType())
	metricMessage := fmt.Sprintf("%s metrics %s", logging.HandlerTypeRestore, id)
	// redefine context cancel.
	ctx, cancel := context.WithCancel(ctx)

	// Channel for transferring readers.
	readersCh := make(chan models.File)
	// Channel for processing errors from readers or writers.
	errorsCh := make(chan error, 1)

	stats := models.NewRestoreStats()

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

	readProcessor := newFileReaderProcessor[T](
		reader,
		config,
		kbpsCollector,
		readersCh,
		errorsCh,
		logger,
	)

	writeProcessor := newRecordWriterProcessor[T](
		aerospikeClient,
		config,
		stats,
		rpsCollector,
		infoClient,
		logger,
	)

	limiter, err := bandwidth.NewLimiter(config.Bandwidth)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create bandwidth limiter: %w", err)
	}

	return &RestoreHandler[T]{
		ctx:            ctx,
		cancel:         cancel,
		readProcessor:  readProcessor,
		writeProcessor: writeProcessor,
		config:         config,
		stats:          stats,
		id:             id,
		logger:         logger,
		limiter:        limiter,
		errors:         errorsCh,
		done:           make(chan struct{}, 1),
		rpsCollector:   rpsCollector,
		kbpsCollector:  kbpsCollector,
	}, nil
}

func (rh *RestoreHandler[T]) run() {
	rh.wg.Add(1)
	rh.stats.Start()

	go doWork(rh.errors, rh.done, rh.logger, func() error {
		defer rh.wg.Done()

		return rh.restore(rh.ctx)
	})
}

func (rh *RestoreHandler[T]) restore(ctx context.Context) error {
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

func (rh *RestoreHandler[T]) restoreMetadata(ctx context.Context) error {
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

func (rh *RestoreHandler[T]) runPipeline(ctx context.Context, dataReaders []pipe.Reader[T]) error {
	dataWriters, err := rh.writeProcessor.newDataWriters(ctx)
	if err != nil {
		return fmt.Errorf("failed to create writer workers: %w", err)
	}

	composeProcessor, err := rh.getComposeProcessor(ctx)
	if err != nil {
		return fmt.Errorf("failed to create compose processor: %w", err)
	}

	pipelineMode := rh.getPipelineMode(len(dataReaders), len(dataWriters))

	pl, err := pipe.NewPipe(
		composeProcessor,
		dataReaders,
		dataWriters,
		rh.limiter,
		pipelineMode,
	)
	if err != nil {
		return err
	}

	// Assign, so we can get pl stats.
	rh.pl.Store(pl)

	return pl.Run(ctx)
}

func (rh *RestoreHandler[T]) getPipelineMode(numReaders, numWriters int) pipe.FanoutStrategy {
	switch {
	case rh.config.EncoderType == EncoderTypeASBX, numReaders == numWriters:
		return pipe.Fixed
	default:
		return pipe.RoundRobin
	}
}

func (rh *RestoreHandler[T]) getComposeProcessor(ctx context.Context) (pipe.ProcessorCreator[T], error) {
	switch rh.config.EncoderType {
	case EncoderTypeASB:
		// Namespace Source and Destination
		var nsSource, nsDest *string
		if rh.config.Namespace != nil {
			nsSource = rh.config.Namespace.Source
			nsDest = rh.config.Namespace.Destination
		}

		return newDataProcessor[T](
			processors.NewRecordCounter[T](&rh.stats.ReadRecords),
			processors.NewSizeCounter[T](&rh.stats.TotalBytesRead),
			processors.NewFilterByType[T](
				rh.config.NoRecords,
				rh.config.NoIndexes,
				rh.config.NoUDFs,
				&rh.stats.RecordsSkipped,
			),
			processors.NewFilterBySet[T](rh.config.SetList, &rh.stats.RecordsSkipped),
			processors.NewFilterByBin[T](rh.config.BinList, &rh.stats.RecordsSkipped),
			processors.NewChangeNamespace[T](nsSource, nsDest),
			processors.NewExpirationSetter[T](&rh.stats.RecordsExpired, rh.config.ExtraTTL, rh.logger),
			processors.NewTPSLimiter[T](ctx, rh.config.RecordsPerSecond),
		), nil

	case EncoderTypeASBX:
		return newDataProcessor[T](
			processors.NewSizeCounter[T](&rh.stats.TotalBytesRead),
			processors.NewTokenCounter[T](&rh.stats.ReadRecords),
			processors.NewTPSLimiter[T](ctx, rh.config.RecordsPerSecond),
		), nil

	default:
		return nil, fmt.Errorf("unknown encoder type: %d", rh.config.EncoderType)
	}
}

// GetStats returns the stats of the restore job.
func (rh *RestoreHandler[T]) GetStats() *models.RestoreStats {
	return rh.stats
}

// Wait waits for the restore job to complete and returns an error if the job failed.
func (rh *RestoreHandler[T]) Wait(ctx context.Context) error {
	var err error

	select {
	case <-rh.ctx.Done():
		// Wait for global context.
		err = rh.ctx.Err()
	case <-ctx.Done():
		// Process local context.
		rh.cancel()

		err = ctx.Err()
	case err = <-rh.errors:
		// On error, we cancel global context.
		// To stop all goroutines and prevent leaks.
		rh.cancel()
	case <-rh.done: // Success
	}

	// Wait when all routines ended.
	rh.wg.Wait()

	rh.cleanup()

	return err
}

// GetMetrics returns the metrics of the restore job.
func (rh *RestoreHandler[T]) GetMetrics() *models.Metrics {
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
func (rh *RestoreHandler[T]) cleanup() {
	rh.stats.Stop()
	rh.rpsCollector.Stop()
	rh.kbpsCollector.Stop()

	pl := rh.pl.Load()
	if pl != nil {
		pl.Close()
	}

	rh.pl.Swap(nil)
}
