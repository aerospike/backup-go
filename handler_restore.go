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
	"io"
	"log/slog"

	"github.com/aerospike/backup-go/internal/logging"
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
	StreamFiles(context.Context, chan<- io.ReadCloser, chan<- error)

	// StreamFile creates a single file reader and sends io.Readers to the `readersCh`
	// In case of an error, it is sent to the `errorsCh` channel.
	// Must be run in a goroutine `go rh.reader.StreamFile()`.
	StreamFile(ctx context.Context, filename string, readersCh chan<- io.ReadCloser, errorsCh chan<- error)

	// GetType returns the type of storage. Used in logging.
	GetType() string
}

// RestoreHandler handles a restore job using the given reader.
type RestoreHandler[T models.TokenConstraint] struct {
	id string
	// Global backup context for a whole restore process.
	ctx    context.Context
	cancel context.CancelFunc

	readProcessor  *fileReaderProcessor[T]
	writeProcessor *recordWriterProcessor[T]
	config         *RestoreConfig
	stats          *models.RestoreStats

	logger  *slog.Logger
	limiter *rate.Limiter

	errors chan error
}

// newRestoreHandler creates a new RestoreHandler.
func newRestoreHandler[T models.TokenConstraint](
	ctx context.Context,
	config *RestoreConfig,
	aerospikeClient AerospikeClient,
	logger *slog.Logger,
	reader StreamingReader,
) *RestoreHandler[T] {
	id := uuid.NewString()
	logger = logging.WithHandler(logger, id, logging.HandlerTypeRestore, reader.GetType())
	// redefine context cancel.
	ctx, cancel := context.WithCancel(ctx)

	// Channel for transferring readers.
	readersCh := make(chan io.ReadCloser)
	// Channel for processing errors from readers or writers.
	errorsCh := make(chan error)

	readProcessor := newFileReaderProcessor[T](
		reader,
		config,
		readersCh,
		errorsCh,
		logger,
	)

	stats := models.NewRestoreStats()

	writeProcessor := newRecordWriterProcessor[T](
		aerospikeClient,
		config,
		stats,
		makeBandwidthLimiter(config.Bandwidth),
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

	pl, err := pipeline.NewPipeline(
		pipeline.ModeSingle, nil,
		readWorkers,
		composeProcessor,
		writeWorkers,
	)
	if err != nil {
		return err
	}

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
