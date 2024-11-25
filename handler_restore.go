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

	"github.com/aerospike/backup-go/internal/asinfo"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/internal/processors"
	"github.com/aerospike/backup-go/io/aerospike"
	"github.com/aerospike/backup-go/io/encryption"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
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
type RestoreHandler struct {
	// Global backup context for a whole restore process.
	ctx    context.Context
	cancel context.CancelFunc

	reader          StreamingReader
	config          *RestoreConfig
	aerospikeClient AerospikeClient

	logger  *slog.Logger
	limiter *rate.Limiter
	errors  chan error
	id      string

	stats models.RestoreStats
}

// newRestoreHandler creates a new RestoreHandler.
func newRestoreHandler(
	ctx context.Context,
	config *RestoreConfig,
	ac AerospikeClient,
	logger *slog.Logger,
	reader StreamingReader,
) *RestoreHandler {
	id := uuid.NewString()
	logger = logging.WithHandler(logger, id, logging.HandlerTypeRestore, reader.GetType())
	// redefine context cancel.
	ctx, cancel := context.WithCancel(ctx)

	return &RestoreHandler{
		ctx:             ctx,
		cancel:          cancel,
		config:          config,
		aerospikeClient: ac,
		id:              id,
		logger:          logger,
		reader:          reader,
		limiter:         makeBandwidthLimiter(config.Bandwidth),
	}
}

func (rh *RestoreHandler) startAsync() {
	rh.errors = make(chan error, 1)
	rh.stats.Start()

	go doWork(rh.errors, rh.logger, func() error {
		return rh.restore(rh.ctx)
	})
}

func (rh *RestoreHandler) restore(ctx context.Context) error {
	// Channel for transferring readers.
	readersCh := make(chan io.ReadCloser)
	// Channel for processing errors from readers or writers.
	errorsCh := make(chan error)
	// Channel for signals about successful processing files.
	doneCh := make(chan struct{})

	ctx, cancel := context.WithCancel(ctx)

	// Start lazy file reading.
	go rh.reader.StreamFiles(ctx, readersCh, errorsCh)

	// Start lazy file processing.
	go rh.restoreFromReaders(ctx, readersCh, doneCh, errorsCh)

	// Process errors if we have them.
	select {
	case err := <-errorsCh:
		cancel()
		return fmt.Errorf("failed to restore: %w", err)
	case <-doneCh:
		cancel()
		return nil
	}
}

// restoreFromReaders serving go routine for processing batches.
func (rh *RestoreHandler) restoreFromReaders(
	ctx context.Context, readersCh <-chan io.ReadCloser,
	doneCh chan<- struct{}, errorsCh chan<- error,
) {
	fn := func(r io.ReadCloser) Decoder {
		reader, err := rh.wrapReader(r)
		if err != nil {
			errorsCh <- err
			return nil
		}

		d, err := NewDecoder(rh.config.EncoderType, reader)
		if err != nil {
			errorsCh <- err
			return nil
		}

		return d
	}

	readWorkers := make([]pipeline.Worker[*models.Token], rh.config.Parallel)
	for i := 0; i < rh.config.Parallel; i++ {
		readWorkers[i] = pipeline.NewReadWorker(newTokenReader(readersCh, rh.logger, fn))
	}

	err := rh.runRestorePipeline(ctx, readWorkers)
	if err != nil {
		errorsCh <- err
	}

	rh.logger.Info("Restore done")
	close(doneCh)
}

// GetStats returns the stats of the restore job.
func (rh *RestoreHandler) GetStats() *models.RestoreStats {
	return &rh.stats
}

// Wait waits for the restore job to complete and returns an error if the job failed.
func (rh *RestoreHandler) Wait(ctx context.Context) error {
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

// runRestorePipeline runs the restore job.
func (rh *RestoreHandler) runRestorePipeline(ctx context.Context, readers []pipeline.Worker[*models.Token]) error {
	rh.logger.Debug("running restore base handler")

	writeWorkers := make([]pipeline.Worker[*models.Token], rh.config.MaxAsyncBatches)

	useBatchWrites, err := rh.useBatchWrites()
	if err != nil {
		return err
	}

	for i := 0; i < rh.config.MaxAsyncBatches; i++ {
		writer := aerospike.NewRestoreWriter(
			rh.aerospikeClient,
			rh.config.WritePolicy,
			&rh.stats,
			rh.logger,
			useBatchWrites,
			rh.config.BatchSize,
			rh.config.RetryPolicy,
			rh.config.IgnoreRecordError,
		)

		statsWriter := newWriterWithTokenStats(writer, &rh.stats, rh.logger)
		writeWorkers[i] = pipeline.NewWriteWorker[*models.Token](statsWriter, rh.limiter)
	}

	// Namespace Source and Destination
	var nsSource, nsDest *string
	if rh.config.Namespace != nil {
		nsSource = rh.config.Namespace.Source
		nsDest = rh.config.Namespace.Destination
	}

	composeProcessor := newTokenWorker(processors.NewComposeProcessor(
		processors.NewRecordCounter(&rh.stats.ReadRecords),
		processors.NewSizeCounter(&rh.stats.TotalBytesRead),
		processors.NewFilterByType(rh.config.NoRecords, rh.config.NoIndexes, rh.config.NoUDFs),
		processors.NewFilterBySet(rh.config.SetList, &rh.stats.RecordsSkipped),
		processors.NewFilterByBin(rh.config.BinList, &rh.stats.RecordsSkipped),
		processors.NewChangeNamespace(nsSource, nsDest),
		processors.NewExpirationSetter(&rh.stats.RecordsExpired, rh.config.ExtraTTL, rh.logger),
		processors.NewTPSLimiter[*models.Token](ctx, rh.config.RecordsPerSecond),
	), rh.config.Parallel)

	pl, err := pipeline.NewPipeline(
		newRoutes[*models.Token](false, 3),
		readers,
		composeProcessor,
		writeWorkers,
	)
	if err != nil {
		return err
	}

	return pl.Run(ctx)
}

func (rh *RestoreHandler) useBatchWrites() (bool, error) {
	if rh.config.DisableBatchWrites {
		return false, nil
	}

	infoClient := asinfo.NewInfoClientFromAerospike(rh.aerospikeClient, rh.config.InfoPolicy)

	return infoClient.SupportsBatchWrite()
}

func newTokenWorker(processor processors.TokenProcessor, parallel int) []pipeline.Worker[*models.Token] {
	if parallel > 0 {
		workers := make([]pipeline.Worker[*models.Token], 0, parallel)
		for i := 0; i < parallel; i++ {
			workers = append(workers, pipeline.NewProcessorWorker(processor))
		}

		return workers
	}

	return []pipeline.Worker[*models.Token]{
		pipeline.NewProcessorWorker(processor),
	}
}

// wrapReader applies encryption and compression wrappers to the reader based on the configuration
func (rh *RestoreHandler) wrapReader(reader io.ReadCloser) (io.ReadCloser, error) {
	r, err := newEncryptionReader(rh.config.EncryptionPolicy, rh.config.SecretAgentConfig, reader)
	if err != nil {
		return nil, fmt.Errorf("failed to create encryption reader: %w", err)
	}

	r, err = newCompressionReader(rh.config.CompressionPolicy, r)
	if err != nil {
		return nil, fmt.Errorf("failed to create compression reader: %w", err)
	}

	return r, nil
}

// newCompressionReader returns compression reader for uncompressing backup.
func newCompressionReader(
	policy *CompressionPolicy, reader io.ReadCloser,
) (io.ReadCloser, error) {
	if policy == nil || policy.Mode == CompressNone {
		return reader, nil
	}

	zstdDecoder, err := zstd.NewReader(reader)
	if err != nil {
		return nil, err
	}

	return zstdDecoder.IOReadCloser(), nil
}

// newEncryptionReader returns encryption reader for decrypting backup.
func newEncryptionReader(
	policy *EncryptionPolicy, saConfig *SecretAgentConfig, reader io.ReadCloser,
) (io.ReadCloser, error) {
	if policy == nil {
		return reader, nil
	}

	privateKey, err := ReadPrivateKey(policy, saConfig)
	if err != nil {
		return nil, err
	}

	encryptedReader, err := encryption.NewEncryptedReader(reader, privateKey)
	if err != nil {
		return nil, err
	}

	return encryptedReader, nil
}
