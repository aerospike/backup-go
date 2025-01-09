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

	"github.com/aerospike/backup-go/io/encryption"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"github.com/klauspost/compress/zstd"
)

// fileReaderProcessor configure and create file readers pipelines.
type fileReaderProcessor[T models.TokenConstraint] struct {
	reader StreamingReader
	config *RestoreConfig

	readersCh chan io.ReadCloser
	errorsCh  chan error

	logger *slog.Logger

	parallel int
}

func newFileReaderProcessor[T models.TokenConstraint](
	reader StreamingReader,
	config *RestoreConfig,
	readersCh chan io.ReadCloser,
	errorsCh chan error,
	logger *slog.Logger,
) *fileReaderProcessor[T] {
	logger.Debug("created file reader processor")

	return &fileReaderProcessor[T]{
		reader:    reader,
		config:    config,
		readersCh: readersCh,
		errorsCh:  errorsCh,
		logger:    logger,
		parallel:  config.Parallel,
	}
}

func (fr *fileReaderProcessor[T]) newReadWorkers(ctx context.Context) []pipeline.Worker[T] {
	// Start lazy file reading.
	go fr.reader.StreamFiles(ctx, fr.readersCh, fr.errorsCh)

	fn := func(r io.ReadCloser) Decoder[T] {
		reader, err := fr.wrapReader(r)
		if err != nil {
			fr.errorsCh <- err
			return nil
		}

		d, err := NewDecoder[T](fr.config.EncoderType, reader)
		if err != nil {
			fr.errorsCh <- err
			return nil
		}

		return d
	}

	readWorkers := make([]pipeline.Worker[T], fr.parallel)
	for i := 0; i < fr.parallel; i++ {
		readWorkers[i] = pipeline.NewReadWorker[T](newTokenReader(fr.readersCh, fr.logger, fn))
	}

	return readWorkers
}

// wrapReader applies encryption and compression wrappers to the reader based on the configuration
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
