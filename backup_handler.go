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
	"sync/atomic"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/encoding"
	"github.com/aerospike/backup-go/internal/asinfo"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/internal/writers"
	"github.com/aerospike/backup-go/io/aerospike"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

// WriteFactory provides access to back up storage.
type WriteFactory interface {
	// NewWriter return new writer for backup logic to use.
	// Each call creates new writer, they might be working in parallel.
	// Backup logic will close the writer after backup is done.
	// header func is executed on a writer after creation (on each one in case of multipart file)
	NewWriter(filename string) (io.WriteCloser, error)
	// GetType return type of storage. Used in logging.
	GetType() string
}

// BackupHandler handles a backup job
type BackupHandler struct {
	writeFactory           WriteFactory
	encoder                encoding.Encoder
	config                 *BackupConfig
	aerospikeClient        *a.Client
	logger                 *slog.Logger
	firstFileHeaderWritten *atomic.Bool
	limiter                *rate.Limiter
	errors                 chan error
	id                     string
	stats                  models.BackupStats
}

// newBackupHandler creates a new BackupHandler
func newBackupHandler(config *BackupConfig,
	ac *a.Client, logger *slog.Logger, writeFactory WriteFactory) *BackupHandler {
	id := uuid.NewString()
	logger = logging.WithHandler(logger, id, logging.HandlerTypeBackup, writeFactory.GetType())
	limiter := makeBandwidthLimiter(config.Bandwidth)

	return &BackupHandler{
		config:                 config,
		aerospikeClient:        ac,
		id:                     id,
		logger:                 logger,
		writeFactory:           writeFactory,
		firstFileHeaderWritten: &atomic.Bool{},
		encoder:                config.EncoderFactory.CreateEncoder(config.Namespace),
		limiter:                limiter,
	}
}

// run runs the backup job
// currently this should only be run once
func (bh *BackupHandler) run(ctx context.Context) {
	bh.errors = make(chan error, 1)
	bh.stats.Start()

	go doWork(bh.errors, bh.logger, func() error {
		return bh.backupSync(ctx)
	})
}

func (bh *BackupHandler) backupSync(ctx context.Context) error {
	backupWriters, err := bh.makeWriters(bh.config.Parallel)
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
	err = bh.backupSIndexesAndUdfs(ctx, backupWriters[0])
	if err != nil {
		return err
	}

	if bh.config.NoRecords {
		// no need to run backup handler
		return nil
	}

	writeWorkers := bh.makeWriteWorkers(backupWriters)
	handler := newBackupRecordsHandler(bh.config, bh.aerospikeClient, bh.logger)

	bh.stats.TotalRecords, err = handler.countRecords()
	if err != nil {
		return err
	}

	return handler.run(ctx, writeWorkers, &bh.stats.RecordsReadTotal)
}

func (bh *BackupHandler) makeWriteWorkers(backupWriters []io.WriteCloser) []pipeline.Worker[*models.Token] {
	writeWorkers := make([]pipeline.Worker[*models.Token], bh.config.Parallel)

	for i, w := range backupWriters {
		var dataWriter pipeline.DataWriter[*models.Token] = newTokenWriter(bh.encoder, w, bh.logger)
		dataWriter = newWriterWithTokenStats(dataWriter, &bh.stats, bh.logger)
		writeWorkers[i] = pipeline.NewWriteWorker(dataWriter, bh.limiter)
	}

	return writeWorkers
}

func (bh *BackupHandler) makeWriters(n int) ([]io.WriteCloser, error) {
	backupWriters := make([]io.WriteCloser, n)

	for i := 0; i < n; i++ {
		writer, err := bh.newWriter()
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
// The returned writer may be compressed or encrypted depending on the BackupHandler's configuration.
func (bh *BackupHandler) newWriter() (io.WriteCloser, error) {
	if bh.config.FileLimit > 0 {
		return writers.NewSized(bh.config.FileLimit, bh.newConfiguredWriter)
	}

	return bh.newConfiguredWriter()
}

func (bh *BackupHandler) newConfiguredWriter() (io.WriteCloser, error) {
	filename := bh.encoder.GenerateFilename()

	storageWriter, err := bh.writeFactory.NewWriter(filename)
	if err != nil {
		return nil, err
	}

	countingWriter := writers.NewCountingWriter(storageWriter, &bh.stats.TotalBytesWritten)

	encryptedWriter, err := setEncryption(bh.config.EncryptionPolicy, countingWriter)
	if err != nil {
		return nil, fmt.Errorf("cannot set encryption: %w", err)
	}

	zippedWriter, err := setCompression(bh.config.CompressionPolicy, encryptedWriter)
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

func setEncryption(policy *models.EncryptionPolicy, writer io.WriteCloser) (io.WriteCloser, error) {
	if policy == nil || policy.Mode == models.EncryptNone {
		return writer, nil
	}

	privateKey, err := policy.ReadPrivateKey()
	if err != nil {
		return nil, err
	}

	return writers.NewEncryptedWriter(writer, privateKey)
}

func setCompression(policy *models.CompressionPolicy, writer io.WriteCloser) (io.WriteCloser, error) {
	if policy == nil || policy.Mode == models.CompressNone {
		return writer, nil
	}

	if policy.Mode == models.CompressZSTD {
		return writers.NewCompressedWriter(writer, policy.Level)
	}

	return nil, fmt.Errorf("unknown compression mode %s", policy.Mode)
}

func makeBandwidthLimiter(bandwidth int) *rate.Limiter {
	if bandwidth > 0 {
		return rate.NewLimiter(rate.Limit(bandwidth), bandwidth)
	}

	return nil
}

func (bh *BackupHandler) backupSIndexesAndUdfs(
	ctx context.Context,
	writer io.WriteCloser,
) error {
	if !bh.config.NoIndexes {
		err := bh.backupSIndexes(ctx, writer)
		if err != nil {
			return err
		}
	}

	if !bh.config.NoUDFs {
		err := bh.backupUDFs(ctx, writer)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetStats returns the stats of the backup job
func (bh *BackupHandler) GetStats() *models.BackupStats {
	return &bh.stats
}

// Wait waits for the backup job to complete and returns an error if the job failed
func (bh *BackupHandler) Wait(ctx context.Context) error {
	defer func() {
		bh.stats.Stop()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-bh.errors:
		return err
	}
}

func (bh *BackupHandler) backupSIndexes(
	ctx context.Context,
	writer io.Writer,
) error {
	infoClient, err := asinfo.NewInfoClientFromAerospike(bh.aerospikeClient, bh.config.InfoPolicy)
	if err != nil {
		return err
	}

	reader := aerospike.NewSIndexReader(infoClient, bh.config.Namespace, bh.logger)
	sindexReadWorker := pipeline.NewReadWorker[*models.Token](reader)

	sindexWriter := pipeline.DataWriter[*models.Token](newTokenWriter(bh.encoder, writer, bh.logger))
	sindexWriter = newWriterWithTokenStats(sindexWriter, &bh.stats, bh.logger)
	sindexWriteWorker := pipeline.NewWriteWorker(sindexWriter, bh.limiter)

	sindexPipeline := pipeline.NewPipeline[*models.Token](
		[]pipeline.Worker[*models.Token]{sindexReadWorker},
		[]pipeline.Worker[*models.Token]{sindexWriteWorker},
	)

	err = sindexPipeline.Run(ctx)
	if err != nil {
		bh.logger.Error("failed to backup secondary indexes", "error", err)
	}

	return err
}

func (bh *BackupHandler) backupUDFs(
	ctx context.Context,
	writer io.Writer,
) error {
	infoClient, err := asinfo.NewInfoClientFromAerospike(bh.aerospikeClient, bh.config.InfoPolicy)
	if err != nil {
		return err
	}

	reader := aerospike.NewUDFReader(infoClient, bh.logger)
	udfReadWorker := pipeline.NewReadWorker[*models.Token](reader)

	udfWriter := pipeline.DataWriter[*models.Token](newTokenWriter(bh.encoder, writer, bh.logger))
	udfWriter = newWriterWithTokenStats(udfWriter, &bh.stats, bh.logger)
	udfWriteWorker := pipeline.NewWriteWorker(udfWriter, bh.limiter)

	udfPipeline := pipeline.NewPipeline[*models.Token](
		[]pipeline.Worker[*models.Token]{udfReadWorker},
		[]pipeline.Worker[*models.Token]{udfWriteWorker},
	)

	err = udfPipeline.Run(ctx)
	if err != nil {
		bh.logger.Error("failed to backup UDFs", "error", err)
	}

	return err
}
