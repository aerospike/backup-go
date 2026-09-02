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
	"errors"
	"io"
	"log/slog"

	"github.com/aerospike/backup-go/models"
)

// tokenReader satisfies the DataReader interface.
// It reads data as tokens using a Decoder.
type tokenReader struct {
	readersCh     <-chan models.File
	decoder       Decoder
	logger        *slog.Logger
	newDecoderFn  func(io.ReadCloser, string) (Decoder, error)
	currentReader io.Closer
}

// newTokenReader creates a new tokenReader.
func newTokenReader(
	readersCh <-chan models.File,
	logger *slog.Logger,
	newDecoderFn func(io.ReadCloser, string) (Decoder, error),
) *tokenReader {
	return &tokenReader{
		readersCh:    readersCh,
		newDecoderFn: newDecoderFn,
		logger:       logger,
	}
}

// Read reads the next token from the token reader.
func (tr *tokenReader) Read(ctx context.Context) (*models.Token, error) {
	for {
		if tr.decoder != nil {
			token, err := tr.decoder.NextToken()

			switch {
			case ctx.Err() != nil:
				return nil, ctx.Err()
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

			var (
				err error
			)

			// Assign the new reader
			tr.currentReader = file.Reader

			tr.decoder, err = tr.newDecoderFn(file.Reader, file.Name)
			if err != nil {
				return nil, err
			}
		}
	}
}

// Close satisfies the pipe.Reader interface.
// It is a no-op for tokenReader.
func (tr *tokenReader) Close() {
	tr.logger.Debug("closed token reader")
}
