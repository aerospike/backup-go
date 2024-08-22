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
	"sync/atomic"

	"github.com/aerospike/backup-go/internal/asinfo"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/io/aerospike"
	"github.com/aerospike/backup-go/io/compression"
	"github.com/aerospike/backup-go/io/counter"
	"github.com/aerospike/backup-go/io/encryption"
	"github.com/aerospike/backup-go/io/sized"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"github.com/google/uuid"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)

// Writer provides access to backup storage.
// Exported for integration tests.
type Writer interface {
	// NewWriter returns new writer for backup logic to use. Each call creates
	// a new writer, they might be working in parallel. Backup logic will close
	// the writer after backup is done. Header func is executed on a writer
	// after creation (on each one in case of multipart file).
	NewWriter(ctx context.Context, filename string) (io.WriteCloser, error)
	// GetType returns the type of storage. Used in logging.
	GetType() string
}

// BackupHandler handles a backup job.
type BackupHandler struct {
	// Global backup context for a whole backup process.
	ctx       context.Context
	ctxCancel context.CancelFunc

	writer          Writer
	encoder         Encoder
	config          *BackupConfig
	aerospikeClient AerospikeClient

	logger                 *slog.Logger
	firstFileHeaderWritten *atomic.Bool
	limiter                *rate.Limiter
	infoClient             *asinfo.InfoClient
	scanLimiter            *semaphore.Weighted
	errors                 chan error
	id                     string

	stats models.BackupStats
}

// newBackupHandler creates a new BackupHandler.
func newBackupHandler(
	ctx context.Context,
	config *BackupConfig,
	ac AerospikeClient,
	logger *slog.Logger,
	writer Writer,
	scanLimiter *semaphore.Weighted,
) *BackupHandler {
	id := uuid.NewString()
	logger = logging.WithHandler(logger, id, logging.HandlerTypeBackup, writer.GetType())
	limiter := makeBandwidthLimiter(config.Bandwidth)

	// redefine context cancel.
	ctx, ctxCancel := context.WithCancel(ctx)

	return &BackupHandler{
		ctx:                    ctx,
		ctxCancel:              ctxCancel,
		config:                 config,
		aerospikeClient:        ac,
		id:                     id,
		logger:                 logger,
		writer:                 writer,
		firstFileHeaderWritten: &atomic.Bool{},
		encoder:                NewEncoder(config.EncoderType, config.Namespace),
		limiter:                limiter,
		infoClient:             asinfo.NewInfoClientFromAerospike(ac, config.InfoPolicy),
		scanLimiter:            scanLimiter,
	}
}

// run runs the backup job.
// currently this should only be run once.
func (bh *BackupHandler) run() {
	bh.errors = make(chan error, 1)
	bh.stats.Start()

	go doWork(bh.errors, bh.logger, func() error {
		return bh.backupSync(bh.ctx)
	})
}

func (bh *BackupHandler) backupSync(ctx context.Context) error {
	backupWriters, err := bh.makeWriters(ctx, bh.config.Parallel)
	if err != nil {
		return err
	}

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

	writeWorkers := bh.makeWriteWorkers(backupWriters)
	handler := newBackupRecordsHandler(bh.config, bh.aerospikeClient, bh.logger, bh.scanLimiter)

	bh.stats.TotalRecords, err = handler.countRecords(ctx, bh.infoClient)
	if err != nil {
		return err
	}

	return handler.run(ctx, writeWorkers, &bh.stats.ReadRecords)
}

func (bh *BackupHandler) makeWriteWorkers(
	backupWriters []io.WriteCloser,
) []pipeline.Worker[*models.Token] {
	writeWorkers := make([]pipeline.Worker[*models.Token], bh.config.Parallel)

	for i, w := range backupWriters {
		var dataWriter pipeline.DataWriter[*models.Token] = newTokenWriter(bh.encoder, w, bh.logger)
		dataWriter = newWriterWithTokenStats(dataWriter, &bh.stats, bh.logger)
		writeWorkers[i] = pipeline.NewWriteWorker(dataWriter, bh.limiter)
	}

	return writeWorkers
}

func (bh *BackupHandler) makeWriters(ctx context.Context, n int) ([]io.WriteCloser, error) {
	backupWriters := make([]io.WriteCloser, n)

	for i := 0; i < n; i++ {
		writer, err := bh.newWriter(ctx)
		if err != nil {
			return nil, err
		}

		backupWriters[i] = writer
	}

	return backupWriters, nil
}

func closeWriters(backupWriters []io.WriteCloser, logger *slog.Logger) {
	for _, w := range backupWriters {
		if err := w.Close(); err != nil {
			logger.Error("failed to close backup file", "error", err)
		}
	}
}

// newWriter creates a new writer based on the current configuration.
// If FileLimit is set, it returns a sized writer limited to FileLimit bytes.
// The returned writer may be compressed or encrypted depending on the BackupHandler's
// configuration.
func (bh *BackupHandler) newWriter(ctx context.Context) (io.WriteCloser, error) {
	if bh.config.FileLimit > 0 {
		return sized.NewWriter(ctx, bh.config.FileLimit, bh.newConfiguredWriter)
	}

	return bh.newConfiguredWriter(ctx)
}

func (bh *BackupHandler) newConfiguredWriter(ctx context.Context) (io.WriteCloser, error) {
	filename := bh.encoder.GenerateFilename()

	storageWriter, err := bh.writer.NewWriter(ctx, filename)
	if err != nil {
		return nil, err
	}

	countingWriter := counter.NewWriter(storageWriter, &bh.stats.BytesWritten)

	encryptedWriter, err := newEncryptionWriter(
		bh.config.EncryptionPolicy,
		bh.config.SecretAgentConfig,
		countingWriter,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot set encryption: %w", err)
	}

	zippedWriter, err := newCompressionWriter(bh.config.CompressionPolicy, encryptedWriter)
	if err != nil {
		return nil, err
	}

	_, err = zippedWriter.Write(bh.encoder.GetHeader())
	if err != nil {
		return nil, err
	}

	bh.stats.IncFiles()

	return zippedWriter, nil
}

// newCompressionWriter returns compression writer for compressing backup.
func newCompressionWriter(
	policy *CompressionPolicy, writer io.WriteCloser,
) (io.WriteCloser, error) {
	if policy == nil || policy.Mode == CompressNone {
		return writer, nil
	}

	if policy.Mode == CompressZSTD {
		return compression.NewWriter(writer, policy.Level)
	}

	return nil, fmt.Errorf("unknown compression mode %s", policy.Mode)
}

// newEncryptionWriter returns encryption writer for encrypting backup.
func newEncryptionWriter(
	policy *EncryptionPolicy, secretAgent *SecretAgentConfig, writer io.WriteCloser,
) (io.WriteCloser, error) {
	if policy == nil || policy.Mode == EncryptNone {
		return writer, nil
	}

	privateKey, err := policy.ReadPrivateKey(secretAgent)
	if err != nil {
		return nil, err
	}

	return encryption.NewWriter(writer, privateKey)
}

func makeBandwidthLimiter(bandwidth int) *rate.Limiter {
	if bandwidth > 0 {
		return rate.NewLimiter(rate.Limit(bandwidth), bandwidth)
	}

	return nil
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

// GetStats returns the stats of the backup job
func (bh *BackupHandler) GetStats() *models.BackupStats {
	return &bh.stats
}

// Wait waits for the backup job to complete and returns an error if the job failed.
func (bh *BackupHandler) Wait(ctx context.Context) error {
	defer func() {
		bh.stats.Stop()
	}()

	select {
	case <-bh.ctx.Done():
		// Wait for global context.
		return bh.ctx.Err()
	case <-ctx.Done():
		// Process local context.
		bh.ctxCancel()
		return ctx.Err()
	case err := <-bh.errors:
		return err
	}
}

func (bh *BackupHandler) backupSIndexes(
	ctx context.Context,
	writer io.Writer,
) error {
	reader := aerospike.NewSIndexReader(bh.infoClient, bh.config.Namespace, bh.logger)
	sindexReadWorker := pipeline.NewReadWorker[*models.Token](reader)

	sindexWriter := pipeline.DataWriter[*models.Token](newTokenWriter(bh.encoder, writer, bh.logger))
	sindexWriter = newWriterWithTokenStats(sindexWriter, &bh.stats, bh.logger)
	sindexWriteWorker := pipeline.NewWriteWorker(sindexWriter, bh.limiter)

	sindexPipeline := pipeline.NewPipeline[*models.Token](
		[]pipeline.Worker[*models.Token]{sindexReadWorker},
		[]pipeline.Worker[*models.Token]{sindexWriteWorker},
	)

	return sindexPipeline.Run(ctx)
}

func (bh *BackupHandler) backupUDFs(
	ctx context.Context,
	writer io.Writer,
) error {
	reader := aerospike.NewUDFReader(bh.infoClient, bh.logger)
	udfReadWorker := pipeline.NewReadWorker[*models.Token](reader)

	udfWriter := pipeline.DataWriter[*models.Token](newTokenWriter(bh.encoder, writer, bh.logger))
	udfWriter = newWriterWithTokenStats(udfWriter, &bh.stats, bh.logger)
	udfWriteWorker := pipeline.NewWriteWorker(udfWriter, bh.limiter)

	udfPipeline := pipeline.NewPipeline[*models.Token](
		[]pipeline.Worker[*models.Token]{udfReadWorker},
		[]pipeline.Worker[*models.Token]{udfWriteWorker},
	)

	return udfPipeline.Run(ctx)
}
