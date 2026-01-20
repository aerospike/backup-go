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

	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/internal/util/files"
	"github.com/aerospike/backup-go/io/compression"
	"github.com/aerospike/backup-go/io/counter"
	"github.com/aerospike/backup-go/io/encryption"
	"github.com/aerospike/backup-go/io/lazy"
	"github.com/aerospike/backup-go/io/sized"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe"
)

const (
	metadataFileNamePrefix = "metadata_"
	// metadataFileID is used to identify metadata file writer.
	metadataFileID = -1
)

// fileWriterProcessor configures and creates file writers pipelines.
type fileWriterProcessor[T models.TokenConstraint] struct {
	prefix          string
	suffixGenerator func() string

	writer            Writer
	encoder           Encoder[T]
	encryptionPolicy  *EncryptionPolicy
	secretAgentConfig *SecretAgentConfig
	compressionPolicy *CompressionPolicy
	state             *State
	stats             *models.BackupStats
	kbpsCollector     *metrics.Collector

	fileLimit uint64
	parallel  int

	logger *slog.Logger
}

// newFileWriterProcessor returns a new file writer processor instance.
func newFileWriterProcessor[T models.TokenConstraint](
	prefix string,
	suffixGenerator func() string,
	writer Writer,
	encoder Encoder[T],
	encryptionPolicy *EncryptionPolicy,
	secretAgentConfig *SecretAgentConfig,
	compressionPolicy *CompressionPolicy,
	state *State,
	stats *models.BackupStats,
	kbpsCollector *metrics.Collector,
	fileLimit uint64,
	parallel int,
	logger *slog.Logger,
) (*fileWriterProcessor[T], error) {
	logger.Debug("created new file writer processor")

	p := &fileWriterProcessor[T]{
		prefix:            prefix,
		suffixGenerator:   suffixGenerator,
		writer:            writer,
		encoder:           encoder,
		encryptionPolicy:  encryptionPolicy,
		secretAgentConfig: secretAgentConfig,
		compressionPolicy: compressionPolicy,
		state:             state,
		stats:             stats,
		kbpsCollector:     kbpsCollector,
		fileLimit:         fileLimit,
		parallel:          parallel,
		logger:            logger,
	}

	// Check writer and parallelism.
	if p.isSingleFileBackup() && parallel > 1 {
		return nil, fmt.Errorf("parallel running for single file is not allowed")
	}

	return p, nil
}

// newDataWriters returns:
//   - Raw writers that satisfy io.WriteCloser interface.
//     That is used for metadata backup in case of backup to one file.
//     As we need to use the same writer for metadata and records.
//     In the case of backup to directory, it won't be used.
//   - Initialized pipeline workers pipe.Writer[T] that should be passed directly to the pipeline,
//     for records data backup.
//   - Error if any.
func (fw *fileWriterProcessor[T]) newDataWriters(ctx context.Context) ([]io.WriteCloser, []pipe.Writer[T], error) {
	writers, err := fw.newWriters(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create storage writers: %w", err)
	}

	dataWriters := make([]pipe.Writer[T], len(writers))

	for i, writer := range writers {
		dataWriters[i] = fw.createDataWriter(writer, i)
	}

	fw.logger.Debug("created data writers", slog.Int("count", len(writers)))

	return writers, dataWriters, nil
}

// newMetaWriter creates a new writer for metadata based on the current configuration.
// In the case of backup to one file, it returns the same writer that was passed.
// Otherwise, it creates a separate writer for metadata.
func (fw *fileWriterProcessor[T]) newMetaWriter(ctx context.Context, writer io.WriteCloser) (io.WriteCloser, error) {
	// If it is backup to file, we return the same writer.
	if fw.isSingleFileBackup() {
		return writer, nil
	}
	// Else we will init separate writer for metadata.
	w, err := fw.newWriter(ctx, metadataFileID)
	if err != nil {
		return nil, fmt.Errorf("failed to create meta writer: %w", err)
	}

	return metrics.NewWriter(w, fw.kbpsCollector), nil
}

// createDataWriter creates a single data writer with state info if available.
func (fw *fileWriterProcessor[T]) createDataWriter(writer io.WriteCloser, n int) pipe.Writer[T] {
	sInfo := fw.getStateInfo(n)
	tWriter := newTokenWriter(fw.encoder, writer, fw.logger.With(slog.Int("writer", n)), sInfo)

	return newWriterWithTokenStats(tWriter, fw.stats, fw.logger)
}

// getStateInfo returns state info for the given index if state is available.
func (fw *fileWriterProcessor[T]) getStateInfo(n int) *stateInfo {
	if fw.state == nil {
		return nil
	}

	return newStateInfo(fw.state.RecordsStateChan, n)
}

// newWriters returns a slice of configured writers.
func (fw *fileWriterProcessor[T]) newWriters(ctx context.Context) ([]io.WriteCloser, error) {
	writers := make([]io.WriteCloser, fw.parallel)

	for i := range fw.parallel {
		writer, err := fw.newWriter(ctx, i)
		if err != nil {
			return nil, fmt.Errorf("failed to create writer: %w", err)
		}
		// Create a writer with metrics.
		writers[i] = metrics.NewWriter(writer, fw.kbpsCollector)
	}

	fw.logger.Debug("created new file writers", slog.Int("writersNumber", len(writers)))

	return writers, nil
}

// newWriter creates a new writer based on the current configuration.
// If FileLimit is set, it returns a sized writer limited to FileLimit bytes.
// The returned writer may be compressed or encrypted depending on the BackupHandler's
// configuration.
func (fw *fileWriterProcessor[T]) newWriter(ctx context.Context, n int,
) (io.WriteCloser, error) {
	var saveCommandChan chan int
	if fw.state != nil {
		saveCommandChan = fw.state.SaveCommandChan
	}

	if fw.fileLimit > 0 {
		return sized.NewWriter(ctx, n, saveCommandChan, fw.fileLimit, fw.configureWriter)
	}

	return lazy.NewWriter(ctx, n, fw.configureWriter)
}

// configureWriter returns configured writer.
func (fw *fileWriterProcessor[T]) configureWriter(ctx context.Context, n int, sizeCounter *atomic.Uint64,
) (io.WriteCloser, error) {
	// Generate file name.
	filename := fw.getFileName(n)

	// Create a file writer.
	storageWriter, err := fw.writer.NewWriter(ctx, filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage writer: %w", err)
	}

	// Apply encryption (if it is enabled).
	encryptedWriter, err := newEncryptionWriter(
		fw.encryptionPolicy,
		fw.secretAgentConfig,
		counter.NewWriter(storageWriter, &fw.stats.BytesWritten, sizeCounter),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to set encryption: %w", err)
	}

	// Apply compression (if it is enabled).
	compressedWriter, err := newCompressionWriter(fw.compressionPolicy, encryptedWriter)
	if err != nil {
		return nil, fmt.Errorf("failed to set compression: %w", err)
	}

	num, err := files.GetFileNumber(filename)
	if err != nil {
		return nil, err
	}

	// Is it a metadata file?
	isRecords := n != metadataFileID

	// Write file header.
	_, err = compressedWriter.Write(fw.encoder.GetHeader(num, isRecords))
	if err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	// Increase file counter.
	fw.stats.IncFiles()

	return compressedWriter, nil
}

// getFileName generates a file name based on the current configuration.
func (fw *fileWriterProcessor[T]) getFileName(n int) string {
	// If it is a single file backup, we don't need to generate a file name.
	if fw.isSingleFileBackup() && n >= 0 {
		return ""
	}

	var prefix string

	switch {
	case n == metadataFileID:
		// For metadata writer create a separate file with a metadata prefix.
		prefix = metadataFileNamePrefix
	case fw.prefix != "":
		// If prefix is set, we use it as a prefix.
		prefix = fw.prefix
	default:
		// If the prefix is not set (for .asbx files prefix must be empty), we use the default one: <worker number>_
		prefix = fmt.Sprintf("%d_", n)
	}

	// Generate file name.
	return fw.encoder.GenerateFilename(prefix, fw.suffixGenerator())
}

// isSingleFileBackup returns true if the backup is single file backup.
func (fw *fileWriterProcessor[T]) isSingleFileBackup() bool {
	return fw.writer != nil && !fw.writer.GetOptions().IsDir
}

// emptyPrefixSuffix returns empty string, to configure prefix and suffix generator.
func emptyPrefixSuffix() string {
	return ""
}

// newCompressionWriter returns a compression writer for compressing backup.
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

// newEncryptionWriter returns an encryption writer for encrypting backup.
func newEncryptionWriter(
	policy *EncryptionPolicy, saConfig *SecretAgentConfig, writer io.WriteCloser,
) (io.WriteCloser, error) {
	if policy == nil || policy.Mode == EncryptNone {
		return writer, nil
	}

	privateKey, err := readPrivateKey(policy, saConfig)
	if err != nil {
		return nil, err
	}

	return encryption.NewWriter(writer, privateKey)
}
