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
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/encoding"
	"github.com/aerospike/backup-go/internal/asinfo"
	"github.com/aerospike/backup-go/internal/logging"
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
	NewWriter(namespace string, header func(io.WriteCloser) error) (io.WriteCloser, error)
	// GetType return type of storage. Used in logging.
	GetType() string
}

// BackupHandler handles a backup job
type BackupHandler struct {
	writeFactory           WriteFactory
	config                 *BackupConfig
	aerospikeClient        *a.Client
	logger                 *slog.Logger
	firstFileHeaderWritten *atomic.Bool
	errors                 chan error
	id                     string
	stats                  BackupStats
}

// BackupStats stores the status of a backup job.
// Stats are updated in realtime by backup jobs.
type BackupStats struct {
	start    time.Time
	Duration time.Duration
	tokenStats
	fileCount atomic.Uint64
}

func (b *BackupStats) incFiles() {
	b.fileCount.Add(1)
}

func (b *BackupStats) GetFileCount() uint64 {
	return b.fileCount.Load()
}

// newBackupHandler creates a new BackupHandler
func newBackupHandler(config *BackupConfig,
	ac *a.Client, logger *slog.Logger, writeFactory WriteFactory) *BackupHandler {
	id := uuid.NewString()
	logger = logging.WithHandler(logger, id, logging.HandlerTypeBackup, writeFactory.GetType())

	return &BackupHandler{
		config:                 config,
		aerospikeClient:        ac,
		id:                     id,
		logger:                 logger,
		writeFactory:           writeFactory,
		firstFileHeaderWritten: &atomic.Bool{},
	}
}

// run runs the backup job
// currently this should only be run once
func (bh *BackupHandler) run(ctx context.Context) {
	bh.errors = make(chan error, 1)
	bh.stats.start = time.Now()

	var limiter *rate.Limiter
	if bh.config.Bandwidth > 0 {
		limiter = rate.NewLimiter(rate.Limit(bh.config.Bandwidth), bh.config.Bandwidth)
	}

	go doWork(bh.errors, bh.logger, func() error {
		writeWorkers := make([]*writeWorker[*models.Token], bh.config.Parallel)

		for i := range bh.config.Parallel {
			encoder, err := bh.config.EncoderFactory.CreateEncoder()
			if err != nil {
				return err
			}

			// TODO: add headers logic to endcoder, instead this lambda acrobatics
			writer, err := bh.writeFactory.NewWriter(bh.config.Namespace, func(w io.WriteCloser) error {
				headerLen, err := bh.writeHeader(w, bh.config.Namespace)
				if err != nil {
					return err
				}

				bh.stats.addTotalSize(uint64(headerLen))
				bh.stats.incFiles()

				return nil
			})
			if err != nil {
				return err
			}

			//nolint:gocritic // defer in loop is ok here,
			// we want to close the file after the backup is done
			defer func() {
				if err := writer.Close(); err != nil {
					bh.logger.Error("failed to close backup file", "error", err)
				}
			}()

			// backup secondary indexes and UDFs on the first writer
			// this is done to match the behavior of the
			// backup c tool and keep the backup files more consistent
			// at some point we may want to treat the secondary indexes/UDFs
			// like records and back them up as part of the same pipeline
			// but doing so would cause them to be mixed in with records in the backup file(s)
			if i == 0 {
				err := bh.backupSIndexesAndUdfs(ctx, writer, limiter)
				if err != nil {
					return err
				}
			}

			var dataWriter dataWriter[*models.Token] = newTokenWriter(encoder, writer, bh.logger)
			dataWriter = newWriterWithTokenStats(dataWriter, &bh.stats, bh.logger)
			writeWorkers[i] = newWriteWorkerWithLimit(dataWriter, limiter)
		}

		if bh.config.NoRecords {
			// no need to run backup handler
			return nil
		}

		handler := newBackupRecordsHandler(bh.config, bh.aerospikeClient, bh.logger)

		return handler.run(ctx, writeWorkers, &bh.stats.recordsTotal)
	})
}

func (bh *BackupHandler) backupSIndexesAndUdfs(
	ctx context.Context, writer io.WriteCloser, limiter *rate.Limiter) error {
	if !bh.config.NoIndexes {
		err := backupSIndexes(ctx, bh.aerospikeClient, bh.config, &bh.stats, writer, bh.logger, limiter)
		if err != nil {
			return err
		}
	}

	if !bh.config.NoUDFs {
		err := backupUDFs(ctx, bh.aerospikeClient, bh.config, &bh.stats, writer, bh.logger, limiter)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetStats returns the stats of the backup job
func (bh *BackupHandler) GetStats() *BackupStats {
	return &bh.stats
}

// Wait waits for the backup job to complete and returns an error if the job failed
func (bh *BackupHandler) Wait(ctx context.Context) error {
	defer func() {
		bh.stats.Duration = time.Since(bh.stats.start)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-bh.errors:
		return err
	}
}

func (bh *BackupHandler) writeHeader(writer io.WriteCloser, namespace string) (int, error) {
	if _, ok := bh.config.EncoderFactory.(*encoding.ASBEncoderFactory); ok {
		return writeASBHeader(writer, namespace, bh.firstFileHeaderWritten.Swap(true))
	}

	return 0, nil
}

func backupSIndexes(ctx context.Context, ac *a.Client, config *BackupConfig, stats *BackupStats,
	writer io.Writer, logger *slog.Logger, limiter *rate.Limiter) error {
	infoClient, err := asinfo.NewInfoClientFromAerospike(ac, config.InfoPolicy)
	if err != nil {
		return err
	}

	reader := newSIndexReader(infoClient, config.Namespace, logger)
	sindexReadWorker := newReadWorker[*models.Token](reader)

	sindexEncoder, err := config.EncoderFactory.CreateEncoder()
	if err != nil {
		return err
	}

	var sindexWriter dataWriter[*models.Token] = newTokenWriter(sindexEncoder, writer, logger)
	sindexWriter = newWriterWithTokenStats(sindexWriter, stats, logger)
	sindexWriteWorker := newWriteWorkerWithLimit(sindexWriter, limiter)

	sindexPipeline := pipeline.NewPipeline[*models.Token](
		[]pipeline.Worker[*models.Token]{sindexReadWorker},
		[]pipeline.Worker[*models.Token]{sindexWriteWorker},
	)

	err = sindexPipeline.Run(ctx)
	if err != nil {
		logger.Error("failed to backup secondary indexes", "error", err)
	}

	return err
}

func backupUDFs(ctx context.Context, ac *a.Client, config *BackupConfig, stats *BackupStats,
	writer io.Writer, logger *slog.Logger, limiter *rate.Limiter) error {
	infoClient, err := asinfo.NewInfoClientFromAerospike(ac, config.InfoPolicy)
	if err != nil {
		return err
	}

	reader := newUDFReader(infoClient, logger)
	udfReadWorker := newReadWorker[*models.Token](reader)

	udfEncoder, err := config.EncoderFactory.CreateEncoder()
	if err != nil {
		return err
	}

	var udfWriter dataWriter[*models.Token] = newTokenWriter(udfEncoder, writer, logger)
	udfWriter = newWriterWithTokenStats(udfWriter, stats, logger)
	udfWriteWorker := newWriteWorkerWithLimit(udfWriter, limiter)

	udfPipeline := pipeline.NewPipeline[*models.Token](
		[]pipeline.Worker[*models.Token]{udfReadWorker},
		[]pipeline.Worker[*models.Token]{udfWriteWorker},
	)

	err = udfPipeline.Run(ctx)
	if err != nil {
		logger.Error("failed to backup UDFs", "error", err)
	}

	return err
}
