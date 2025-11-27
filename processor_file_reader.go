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
	"strconv"
	"strings"

	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/io/encoding/asbx"
	"github.com/aerospike/backup-go/io/encryption"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe"
	"github.com/klauspost/compress/zstd"
)

// fileReaderProcessor configures and creates file readers pipelines for restoring data.
type fileReaderProcessor[T models.TokenConstraint] struct {
	reader StreamingReader
	config *ConfigRestore

	// kilobytes per second collector.
	kbpsCollector *metrics.Collector

	readersCh chan models.File
	errorsCh  chan error

	logger *slog.Logger

	parallel int
}

// newFileReaderProcessor returns a new file reader processor.
func newFileReaderProcessor[T models.TokenConstraint](
	reader StreamingReader,
	config *ConfigRestore,
	kbpsCollector *metrics.Collector,
	readersCh chan models.File,
	errorsCh chan error,
	logger *slog.Logger,
) *fileReaderProcessor[T] {
	logger.Debug("created file reader processor")

	return &fileReaderProcessor[T]{
		reader:        reader,
		config:        config,
		kbpsCollector: kbpsCollector,
		readersCh:     readersCh,
		errorsCh:      errorsCh,
		logger:        logger,
		parallel:      config.Parallel,
	}
}

// newDataReaders creates the data readers for restoring data.
func (fr *fileReaderProcessor[T]) newDataReaders(ctx context.Context) []pipe.Reader[T] {
	var skipPrefixes []string
	if fr.config.ApplyMetadataLast {
		skipPrefixes = []string{metadataFileNamePrefix}
	}

	// Start lazy file reading.
	go fr.reader.StreamFiles(ctx, fr.readersCh, fr.errorsCh, skipPrefixes)

	readWorkers := make([]pipe.Reader[T], fr.parallel)

	switch fr.config.EncoderType {
	case EncoderTypeASB:
		for i := 0; i < fr.parallel; i++ {
			readWorkers[i] = newTokenReader(fr.readersCh, fr.logger, fr.initDecoder)
		}
	case EncoderTypeASBX:
		workersReadChans := make([]chan models.File, fr.parallel)

		for i := 0; i < fr.parallel; i++ {
			rCh := make(chan models.File)
			workersReadChans[i] = rCh
			readWorkers[i] = newTokenReader(rCh, fr.logger, fr.initDecoder)
		}

		go distributeFiles(fr.readersCh, workersReadChans, fr.errorsCh)
	}

	return readWorkers
}

// initDecoder initializes the decoder for the given reader.
func (fr *fileReaderProcessor[T]) initDecoder(r io.ReadCloser, fileNumber uint64, fileName string) (Decoder[T], error) {
	reader, err := fr.wrapReader(r)
	if err != nil {
		return nil, err
	}

	reader = metrics.NewReader(reader, fr.kbpsCollector)

	d, err := NewDecoder[T](
		fr.config.EncoderType,
		reader,
		fileNumber,
		fileName,
		fr.config.IgnoreUnknownFields,
		fr.logger,
	)
	if err != nil {
		return nil, err
	}

	return d, nil
}

// newMetadataReaders creates the metadata readers for restoring metadata.
func (fr *fileReaderProcessor[T]) newMetadataReaders(ctx context.Context) []pipe.Reader[T] {
	mdFiles := fr.reader.GetSkipped()

	if len(mdFiles) == 0 {
		return nil
	}

	mdReadersCh := make(chan models.File)

	go func() {
		for i := range mdFiles {
			fr.reader.StreamFile(ctx, mdFiles[i], mdReadersCh, fr.errorsCh)
		}

		close(mdReadersCh)
	}()

	readWorkers := make([]pipe.Reader[T], fr.parallel)
	for i := 0; i < fr.parallel; i++ {
		readWorkers[i] = newTokenReader(mdReadersCh, fr.logger, fr.initDecoder)
	}

	return readWorkers
}

// wrapReader applies encryption and compression wrappers to the reader based on the configuration.
func (fr *fileReaderProcessor[T]) wrapReader(reader io.ReadCloser) (io.ReadCloser, error) {
	r, err := newEncryptionReader(fr.config.EncryptionPolicy, fr.config.SecretAgentConfig, reader)
	if err != nil {
		return nil, fmt.Errorf("failed to create encryption reader: %w", err)
	}

	r, err = newCompressionReader(fr.config.CompressionPolicy, r)
	if err != nil {
		return nil, fmt.Errorf("failed to create compression reader: %w", err)
	}

	return r, nil
}

// newCompressionReader returns a compression reader for uncompressing backup.
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

// newEncryptionReader returns an encryption reader for decrypting backup.
func newEncryptionReader(
	policy *EncryptionPolicy, saConfig *SecretAgentConfig, reader io.ReadCloser,
) (io.ReadCloser, error) {
	if policy == nil {
		return reader, nil
	}

	privateKey, err := readPrivateKey(policy, saConfig)
	if err != nil {
		return nil, err
	}

	encryptedReader, err := encryption.NewEncryptedReader(reader, privateKey)
	if err != nil {
		return nil, err
	}

	return encryptedReader, nil
}

// distributeFiles is only used for asbx restore, to follow the order of files.
// To maintain XDR event order, files must be pre-sorted using util.SortBackupFiles.
// Then they will be distributed to workers based on their prefixes, in suffix order.
// Valid file name:
//
//	<prefix>_<namespace>_<suffix>.asbx
//
// Example:
//
//	4_source-ns1_47.asbx
func distributeFiles(input chan models.File, output []chan models.File, errors chan<- error) {
	if len(output) == 0 {
		errors <- fmt.Errorf("failed to distibute files to 0 channels")
		return
	}

	validator := asbx.NewValidator()

	for file := range input {
		// Skip non asbx files.
		if err := validator.Run(file.Name); err != nil {
			continue
		}

		parts := strings.SplitN(file.Name, "_", 2)

		num, err := strconv.Atoi(parts[0])
		if err != nil {
			errors <- fmt.Errorf("failed to parse distibution file number for file %s: %w", file.Name, err)
			return
		}

		if num > len(output)-1 {
			num = (len(output) - 1) % num
		}

		output[num] <- file
	}

	// Close channels at the end.
	for i := range output {
		close(output[i])
	}
}
