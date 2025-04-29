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
	"github.com/aerospike/backup-go/pipeline"
	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

// StreamingReader provides access to data that should be restored.
type StreamingReader interface {
	// StreamFiles creates readers from files and sends them to the channel.
	// In case of an error, the error is sent to the error channel.
	// Must be run in a goroutine `go rh.reader.StreamFiles(ctx, readersCh, errorsCh)`.
	StreamFiles(context.Context, chan<- models.File, chan<- error)

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
	limiter *rate.Limiter

	pl            *pipeline.Pipeline[T]
	rpsCollector  *metrics.Collector
	kbpsCollector *metrics.Collector

	id     string
	errors chan error
}

// newRestoreHandler creates a new RestoreHandler.
func newRestoreHandler[T models.TokenConstraint](
	ctx context.Context,
	config *ConfigRestore,
	aerospikeClient AerospikeClient,
	logger *slog.Logger,
	reader StreamingReader,
) *RestoreHandler[T] {
	id := uuid.NewString()
	logger = logging.WithHandler(logger, id, logging.HandlerTypeRestore, reader.GetType())
	metricMessage := fmt.Sprintf("%s metrics %s", logging.HandlerTypeRestore, id)
	// redefine context cancel.
	ctx, cancel := context.WithCancel(ctx)

	// Channel for transferring readers.
	readersCh := make(chan models.File)
	// Channel for processing errors from readers or writers.
	errorsCh := make(chan error)

	stats := models.NewRestoreStats()
	rpsCollector := metrics.NewCollector(
		ctx,
		logger,
		metrics.MetricRecordsPerSecond,
		metricMessage,
		config.MetricsEnabled,
	)
	kbpsCollector := metrics.NewCollector(
		ctx,
		logger,
		metrics.MetricKilobytesPerSecond,
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
		makeBandwidthLimiter(config.Bandwidth),
		rpsCollector,
		logger,
	)

	return &RestoreHandler[T]{
		ctx:            ctx,
		cancel:         cancel,
		readProcessor:  readProcessor,
		writeProcessor: writeProcessor,
		config:         config,
		stats:          stats,
		id:             id,
		logger:         logger,
		limiter:        makeBandwidthLimiter(config.Bandwidth),
		errors:         errorsCh,
		rpsCollector:   rpsCollector,
		kbpsCollector:  kbpsCollector,
	}
}

func (rh *RestoreHandler[T]) run() {
	rh.stats.Start()

	go doWork(rh.errors, rh.logger, func() error {
		return rh.restore(rh.ctx)
	})
}

func (rh *RestoreHandler[T]) restore(ctx context.Context) error {
	readWorkers := rh.readProcessor.newReadWorkers(ctx)

	writeWorkers, err := rh.writeProcessor.newWriterWorkers()
	if err != nil {
		return fmt.Errorf("failed to create writer workers: %w", err)
	}

	composeProcessor, err := rh.getComposeProcessor(ctx)
	if err != nil {
		return fmt.Errorf("failed to create compose processor: %w", err)
	}

	pipelineMode := pipeline.ModeSingle
	if rh.config.EncoderType == EncoderTypeASBX {
		pipelineMode = pipeline.ModeParallel
	}

	pl, err := pipeline.NewPipeline(
		pipelineMode, nil,
		readWorkers,
		composeProcessor,
		writeWorkers,
	)
	if err != nil {
		return err
	}

	// Assign, so we can get pl stats.
	rh.pl = pl

	return pl.Run(ctx)
}

func (rh *RestoreHandler[T]) getComposeProcessor(ctx context.Context) ([]pipeline.Worker[T], error) {
	switch rh.config.EncoderType {
	case EncoderTypeASB:
		// Namespace Source and Destination
		var nsSource, nsDest *string
		if rh.config.Namespace != nil {
			nsSource = rh.config.Namespace.Source
			nsDest = rh.config.Namespace.Destination
		}

		return newTokenWorker[T](processors.NewComposeProcessor[T](
			processors.NewRecordCounter[T](&rh.stats.ReadRecords),
			processors.NewSizeCounter[T](&rh.stats.TotalBytesRead),
			processors.NewFilterByType[T](rh.config.NoRecords, rh.config.NoIndexes, rh.config.NoUDFs),
			processors.NewFilterBySet[T](rh.config.SetList, &rh.stats.RecordsSkipped),
			processors.NewFilterByBin[T](rh.config.BinList, &rh.stats.RecordsSkipped),
			processors.NewChangeNamespace[T](nsSource, nsDest),
			processors.NewExpirationSetter[T](&rh.stats.RecordsExpired, rh.config.ExtraTTL, rh.logger),
			processors.NewTPSLimiter[T](ctx, rh.config.RecordsPerSecond),
		), rh.config.Parallel), nil

	case EncoderTypeASBX:
		return newTokenWorker[T](processors.NewComposeProcessor[T](
			processors.NewTokenCounter[T](&rh.stats.ReadRecords),
			processors.NewTPSLimiter[T](ctx, rh.config.RecordsPerSecond),
		), rh.config.Parallel), nil

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
	defer func() {
		rh.stats.Stop()
	}()

	select {
	case <-rh.ctx.Done():
		// Wait for global context.
		return rh.ctx.Err()
	case <-ctx.Done():
		// Process local context.
		rh.cancel()
		return ctx.Err()
	case err := <-rh.errors:
		return err
	}
}

// GetMetrics returns the metrics of the restore job.
func (rh *RestoreHandler[T]) GetMetrics() *models.Metrics {
	if rh == nil {
		return nil
	}

	var pr, pw int
	if rh.pl != nil {
		pr, pw = rh.pl.GetMetrics()
	}

	return models.NewMetrics(
		pr, pw,
		rh.rpsCollector.GetLastResult(),
		rh.kbpsCollector.GetLastResult(),
	)
}
