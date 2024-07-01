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
	"fmt"
	"io"
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/internal/asinfo"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/internal/processors"
	"github.com/aerospike/backup-go/internal/writers"
	"github.com/aerospike/backup-go/io/aerospike"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/time/rate"
)

// ReaderFactory provides access to data that should be restored.
type ReaderFactory interface {
	// Readers returns all available readers of backup data.
	// They will be used in parallel and closed after restore.
	Readers() ([]io.ReadCloser, error) //TODO: use lazy creation
	// GetType return type of storage. Used in logging.
	GetType() string
}

// RestoreHandler handles a restore job using the given readerFactory.
type RestoreHandler struct {
	readerFactory   ReaderFactory
	config          *RestoreConfig
	aerospikeClient *a.Client
	logger          *slog.Logger
	limiter         *rate.Limiter
	errors          chan error
	id              string
	stats           models.RestoreStats
}

// newRestoreHandler creates a new RestoreHandler
func newRestoreHandler(config *RestoreConfig,
	ac *a.Client, logger *slog.Logger, readerFactory ReaderFactory) *RestoreHandler {
	id := uuid.NewString()
	logger = logging.WithHandler(logger, id, logging.HandlerTypeRestore, readerFactory.GetType())

	return &RestoreHandler{
		config:          config,
		aerospikeClient: ac,
		id:              id,
		logger:          logger,
		readerFactory:   readerFactory,
		limiter:         makeBandwidthLimiter(config.Bandwidth),
	}
}

func (rh *RestoreHandler) startAsync(ctx context.Context) {
	rh.errors = make(chan error, 1)
	rh.stats.Start()

	go doWork(rh.errors, rh.logger, func() error {
		return rh.restore(ctx)
	})
}

func (rh *RestoreHandler) restore(ctx context.Context) error {
	readers, err := rh.readerFactory.Readers()
	if err != nil {
		return fmt.Errorf("failed to get readers: %w", err)
	}

	readers, err = SetEncryptionDecoder(rh.config.EncryptionPolicy, readers)
	if err != nil {
		return err
	}

	readers, err = setCompressionDecoder(rh.config.CompressionPolicy, readers)
	if err != nil {
		return err
	}

	totalReaders := len(readers)
	batchSize := rh.config.Parallel

	for start := 0; start < totalReaders; start += batchSize {
		end := min(start+batchSize, totalReaders)
		if err := rh.processBatch(ctx, readers[start:end]); err != nil {
			return fmt.Errorf("failed to process batch: %w", err)
		}
	}

	return nil
}

func setCompressionDecoder(policy *models.CompressionPolicy, readers []io.ReadCloser) ([]io.ReadCloser, error) {
	if policy == nil || policy.Mode == models.CompressNone {
		return readers, nil
	}

	zstdReaders := make([]io.ReadCloser, len(readers))

	for i, reader := range readers {
		zstdDecoder, err := zstd.NewReader(reader)
		if err != nil {
			return nil, err
		}

		zstdReaders[i] = zstdDecoder.IOReadCloser()
	}

	return zstdReaders, nil
}

func SetEncryptionDecoder(policy *models.EncryptionPolicy, readers []io.ReadCloser) ([]io.ReadCloser, error) {
	if policy == nil {
		return readers, nil
	}

	privateKey, err := policy.ReadPrivateKey()
	if err != nil {
		return nil, err
	}

	decryptedReaders := make([]io.ReadCloser, len(readers))

	for i, reader := range readers {
		encryptedReader, err := writers.NewEncryptedReader(reader, privateKey)
		if err != nil {
			return nil, err
		}

		decryptedReaders[i] = encryptedReader
	}

	return decryptedReaders, nil
}

func (rh *RestoreHandler) processBatch(ctx context.Context, rs []io.ReadCloser) error {
	defer rh.closeReaders(rs)

	readWorkers, err := rh.readersToReadWorkers(rs)
	if err != nil {
		return fmt.Errorf("failed to convert readers to read workers: %w", err)
	}

	if err := rh.runRestoreBatch(ctx, readWorkers); err != nil {
		return fmt.Errorf("failed to run restore batch: %w", err)
	}

	return nil
}

func (rh *RestoreHandler) closeReaders(rs []io.ReadCloser) {
	for _, r := range rs {
		if err := r.Close(); err != nil {
			rh.logger.Error("failed to close aerospike backup reader", "error", err)
		}
	}
}

func (rh *RestoreHandler) readersToReadWorkers(readers []io.ReadCloser) (
	[]pipeline.Worker[*models.Token], error) {
	readWorkers := make([]pipeline.Worker[*models.Token], len(readers))

	for i, reader := range readers {
		decoder, err := rh.config.DecoderFactory.CreateDecoder(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to create decoder: %w", err)
		}

		dr := newTokenReader(decoder, rh.logger)
		readWorkers[i] = pipeline.NewReadWorker[*models.Token](dr)
	}

	return readWorkers, nil
}

// GetStats returns the stats of the restore job
func (rh *RestoreHandler) GetStats() *models.RestoreStats {
	return &rh.stats
}

// Wait waits for the restore job to complete and returns an error if the job failed
func (rh *RestoreHandler) Wait(ctx context.Context) error {
	defer func() {
		rh.stats.Stop()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-rh.errors:
		return err
	}
}

// run runs the restore job
func (rh *RestoreHandler) runRestoreBatch(ctx context.Context, readers []pipeline.Worker[*models.Token]) error {
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
		)

		statsWriter := newWriterWithTokenStats(writer, &rh.stats, rh.logger)
		writeWorkers[i] = pipeline.NewWriteWorker[*models.Token](statsWriter, rh.limiter)
	}

	recordCounter := newTokenWorker(processors.NewRecordCounter(&rh.stats.RecordsTotal))
	sizeCounter := newTokenWorker(processors.NewSizeCounter(&rh.stats.TotalBytesRead))
	changeNamespace := newTokenWorker(processors.NewChangeNamespace(rh.config.Namespace))
	ttlSetter := newTokenWorker(processors.NewExpirationSetter(&rh.stats.RecordsExpired, rh.logger))
	binFilter := newTokenWorker(processors.NewFilterByBin(rh.config.BinList, &rh.stats.RecordsSkipped))
	tpsLimiter := newTokenWorker(processors.NewTPSLimiter[*models.Token](ctx, rh.config.RecordsPerSecond))
	tokenTypeFilter := newTokenWorker(
		processors.NewFilterByType(rh.config.NoRecords, rh.config.NoIndexes, rh.config.NoUDFs))
	recordSetFilter := newTokenWorker(processors.NewFilterBySet(rh.config.SetList, &rh.stats.RecordsSkipped))

	job := pipeline.NewPipeline(
		readers,

		// in the pipeline, first all counters.
		recordCounter,
		sizeCounter,

		// filters
		tokenTypeFilter,
		recordSetFilter,
		binFilter,

		// speed limiters.
		tpsLimiter,

		// modifications.
		changeNamespace,
		ttlSetter,

		writeWorkers,
	)

	return job.Run(ctx)
}

func (rh *RestoreHandler) useBatchWrites() (bool, error) {
	if rh.config.DisableBatchWrites {
		return false, nil
	}

	infoClient, err := asinfo.NewInfoClientFromAerospike(rh.aerospikeClient, rh.config.InfoPolicy)
	if err != nil {
		return false, err
	}

	return infoClient.SupportsBatchWrite()
}

func newTokenWorker(processor processors.TokenProcessor) []pipeline.Worker[*models.Token] {
	return []pipeline.Worker[*models.Token]{
		processors.NewProcessorWorker(processor),
	}
}

// **** Restore From Reader Handler ****
