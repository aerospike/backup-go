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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"

	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
)

// tokenReader satisfies the DataReader interface.
// It reads data as tokens using a Decoder.
type tokenReader[T models.TokenConstraint] struct {
	readersCh     <-chan models.File
	decoder       Decoder[T]
	logger        *slog.Logger
	newDecoderFn  func(uint64, io.ReadCloser) Decoder[T]
	currentReader io.Closer
}

// newTokenReader creates a new tokenReader.
func newTokenReader[T models.TokenConstraint](
	readersCh <-chan models.File,
	logger *slog.Logger,
	newDecoderFn func(uint64, io.ReadCloser) Decoder[T],
) *tokenReader[T] {
	return &tokenReader[T]{
		readersCh:    readersCh,
		newDecoderFn: newDecoderFn,
		logger:       logger,
	}
}

func (tr *tokenReader[T]) Read() (T, error) {
	for {
		if tr.decoder != nil {
			token, err := tr.decoder.NextToken()

			switch {
			case err == nil:
				return token, nil
			case errors.Is(err, io.EOF):
				// Current decoder has finished, close the current reader
				if tr.currentReader != nil {
					_ = tr.currentReader.Close()
				}

				tr.decoder = nil
				tr.currentReader = nil
			default:
				return nil, err
			}
		}

		if tr.decoder == nil {
			// We need a new decoder
			file, ok := <-tr.readersCh
			if !ok {
				// Channel is closed, return EOF
				return nil, io.EOF
			}

			num, err := getFileNumber(file.Name)
			if err != nil {
				return nil, err
			}

			// Assign the new reader
			tr.currentReader = file.Reader

			tr.decoder = tr.newDecoderFn(num, file.Reader)
		}
	}
}

// Close satisfies the DataReader interface
// but is a no-op for the tokenReader.
func (tr *tokenReader[T]) Close() {
	tr.logger.Debug("closed token reader")
}

func newTokenWorker[T models.TokenConstraint](processor pipeline.DataProcessor[T], parallel int) []pipeline.Worker[T] {
	if parallel > 0 {
		workers := make([]pipeline.Worker[T], 0, parallel)
		for i := 0; i < parallel; i++ {
			workers = append(workers, pipeline.NewProcessorWorker[T](processor))
		}

		return workers
	}

	return []pipeline.Worker[T]{
		pipeline.NewProcessorWorker[T](processor),
	}
}

func getFileNumber(filename string) (uint64, error) {
	name := strings.TrimSuffix(filename, ".asbx")
	parts := strings.SplitN(name, "_", 3)

	if len(parts) != 3 {
		return 0, fmt.Errorf("invalid file name %q", filename)
	}

	num, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse file number %q: %w", filename, err)
	}

	return num, nil
}
