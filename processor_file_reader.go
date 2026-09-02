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

	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/io/encryption"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe"
	"github.com/klauspost/compress/zstd"
)

// fileReaderProcessor configures and creates file readers pipelines for restoring data.
type fileReaderProcessor struct {
	reader StreamingReader
	config *ConfigRestore

	encryptionKey []byte
	// kilobytes per second collector.
	kbpsCollector *metrics.Collector

	readersCh chan models.File
	errorsCh  chan error

	logger *slog.Logger

	parallel int
}

// newFileReaderProcessor returns a new file reader processor.
// encryptionKey is the key for decryption; pass nil when encryption is disabled.
func newFileReaderProcessor(
	reader StreamingReader,
	config *ConfigRestore,
	encryptionKey []byte,
	kbpsCollector *metrics.Collector,
	readersCh chan models.File,
	errorsCh chan error,
	logger *slog.Logger,
) *fileReaderProcessor {
	logger.Debug("created file reader processor")

	return &fileReaderProcessor{
		reader:        reader,
		config:        config,
		encryptionKey: encryptionKey,
		kbpsCollector: kbpsCollector,
		readersCh:     readersCh,
		errorsCh:      errorsCh,
		logger:        logger,
		parallel:      config.Parallel,
	}
}

// newDataReaders creates the data readers for restoring data.
func (fr *fileReaderProcessor) newDataReaders(ctx context.Context) []pipe.Reader {
	var skipPrefixes []string
	if fr.config.ApplyMetadataLast {
		skipPrefixes = []string{metadataFileNamePrefix}
	}

	// Start lazy file reading.
	go fr.reader.StreamFiles(ctx, fr.readersCh, fr.errorsCh, skipPrefixes)

	readWorkers := make([]pipe.Reader, fr.parallel)

	for i := 0; i < fr.parallel; i++ {
		readWorkers[i] = newTokenReader(fr.readersCh, fr.logger, fr.initDecoder)
	}

	return readWorkers
}

// initDecoder initializes the decoder for the given reader.
func (fr *fileReaderProcessor) initDecoder(r io.ReadCloser, fileName string) (Decoder, error) {
	reader, err := fr.wrapReader(r)
	if err != nil {
		return nil, err
	}

	reader = metrics.NewReader(reader, fr.kbpsCollector)

	d, err := NewDecoder(
		reader,
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
func (fr *fileReaderProcessor) newMetadataReaders(ctx context.Context) []pipe.Reader {
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

	readWorkers := make([]pipe.Reader, fr.parallel)
	for i := 0; i < fr.parallel; i++ {
		readWorkers[i] = newTokenReader(mdReadersCh, fr.logger, fr.initDecoder)
	}

	return readWorkers
}

// wrapReader applies encryption and compression wrappers to the reader based on the configuration.
func (fr *fileReaderProcessor) wrapReader(reader io.ReadCloser) (io.ReadCloser, error) {
	r, err := newEncryptionReader(fr.encryptionKey, reader)
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
// Pass nil encryptionKey when encryption is disabled.
func newEncryptionReader(encryptionKey []byte, reader io.ReadCloser) (io.ReadCloser, error) {
	if encryptionKey == nil {
		return reader, nil
	}

	encryptedReader, err := encryption.NewEncryptedReader(reader, encryptionKey)
	if err != nil {
		return nil, err
	}

	return encryptedReader, nil
}
