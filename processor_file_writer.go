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

	"github.com/aerospike/backup-go/io/counter"
	"github.com/aerospike/backup-go/io/lazy"
	"github.com/aerospike/backup-go/io/sized"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"golang.org/x/time/rate"
)

// fileWriterProcessor configure and creates file writers pipelines.
// TODO: or may be FileWriterHandler?
type fileWriterProcessor[T models.TokenConstraint] struct {
	prefixGenerator func() string
	suffixGenerator func() string

	writer            Writer
	encoder           Encoder[T]
	encryptionPolicy  *EncryptionPolicy
	secretAgentConfig *SecretAgentConfig
	compressionPolicy *CompressionPolicy
	state             *State
	stats             *models.BackupStats
	limiter           *rate.Limiter

	logger *slog.Logger

	saveCommandChan chan int

	fileLimit int64
	parallel  int
}

// newFileWriterProcessor returns new file writer processor instance.
func newFileWriterProcessor[T models.TokenConstraint](
	prefixGenerator func() string,
	suffixGenerator func() string,
	saveCommandChan chan int,
	writer Writer,
	encoder Encoder[T],
	encryptionPolicy *EncryptionPolicy,
	secretAgentConfig *SecretAgentConfig,
	compressionPolicy *CompressionPolicy,
	state *State,
	stats *models.BackupStats,
	limiter *rate.Limiter,
	fileLimit int64,
	parallel int,
	logger *slog.Logger,
) *fileWriterProcessor[T] {
	logger.Debug("created new file writer processor")

	return &fileWriterProcessor[T]{
		prefixGenerator:   prefixGenerator,
		suffixGenerator:   suffixGenerator,
		saveCommandChan:   saveCommandChan,
		writer:            writer,
		encoder:           encoder,
		encryptionPolicy:  encryptionPolicy,
		secretAgentConfig: secretAgentConfig,
		compressionPolicy: compressionPolicy,
		state:             state,
		stats:             stats,
		limiter:           limiter,
		fileLimit:         fileLimit,
		parallel:          parallel,
		logger:            logger,
	}
}

// newWriteWorkers returns a pipeline writing workers' for writers.
func (fw *fileWriterProcessor[T]) newWriteWorkers(writers []io.WriteCloser,
) []pipeline.Worker[T] {
	writeWorkers := make([]pipeline.Worker[T], len(writers))

	for i, writer := range writers {
		var dataWriter pipeline.DataWriter[T] = newTokenWriter(fw.encoder, writer, fw.logger, nil)

		if fw.state != nil {
			stInfo := newStateInfo(fw.state.RecordsStateChan, i)
			dataWriter = newTokenWriter(fw.encoder, writer, fw.logger, stInfo)
		}

		dataWriter = newWriterWithTokenStats(dataWriter, fw.stats, fw.logger)
		writeWorkers[i] = pipeline.NewWriteWorker(dataWriter, fw.limiter)
	}

	fw.logger.Debug("created new writers pipeline", slog.Int("writersNumber", len(writers)))

	return writeWorkers
}

// newWriters returns slice of configured writers.
func (fw *fileWriterProcessor[T]) newWriters(ctx context.Context) ([]io.WriteCloser, error) {
	writers := make([]io.WriteCloser, fw.parallel)

	for i := range fw.parallel {
		writer, err := fw.newWriter(ctx, i, fw.saveCommandChan, fw.fileLimit)
		if err != nil {
			return nil, fmt.Errorf("failed to create writer: %w", err)
		}

		writers[i] = writer
	}

	fw.logger.Debug("created new file writers", slog.Int("writersNumber", len(writers)))

	return writers, nil
}

// newWriter returns new configured writer.
func (fw *fileWriterProcessor[T]) newWriter(ctx context.Context, n int, saveCommandChan chan int, fileLimit int64,
) (io.WriteCloser, error) {
	// TODO: check this part in ordinary scan with state saving feature.
	if fileLimit > 0 {
		return sized.NewWriter(ctx, n, saveCommandChan, fileLimit, fw.configureWriter)
	}

	return lazy.NewWriter(ctx, fw.configureWriter)
}

// configureWriter returns configured writer.
func (fw *fileWriterProcessor[T]) configureWriter(ctx context.Context) (io.WriteCloser, error) {
	// Generate file name.
	filename := fw.encoder.GenerateFilename(fw.prefixGenerator(), fw.suffixGenerator())

	// Create a file writer.
	storageWriter, err := fw.writer.NewWriter(ctx, filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage writer: %w", err)
	}

	// Apply encryption (if it is enabled).
	encryptedWriter, err := newEncryptionWriter(
		fw.encryptionPolicy,
		fw.secretAgentConfig,
		counter.NewWriter(storageWriter, &fw.stats.BytesWritten),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to set encryption: %w", err)
	}

	// Apply compression (if it is enabled).
	compressedWriter, err := newCompressionWriter(fw.compressionPolicy, encryptedWriter)
	if err != nil {
		return nil, fmt.Errorf("failed to set compression: %w", err)
	}

	// Write file header.
	_, err = compressedWriter.Write(fw.encoder.GetHeader())
	if err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	// Increase file counter.
	fw.stats.IncFiles()

	return compressedWriter, nil
}

// emptyPrefixSuffix returns empty string, to configure prefix and suffix generator.
func emptyPrefixSuffix() string {
	return ""
}
