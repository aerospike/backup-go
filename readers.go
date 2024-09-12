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
	"io"
	"log/slog"

	"github.com/aerospike/backup-go/models"
)

// tokenReader satisfies the DataReader interface.
// It reads data as tokens using a Decoder.
type tokenReader struct {
	readersCh    <-chan io.ReadCloser
	decoder      Decoder
	logger       *slog.Logger
	newDecoderFn func(io.ReadCloser) Decoder
}

// newTokenReader creates a new tokenReader.
func newTokenReader(
	readersCh <-chan io.ReadCloser,
	logger *slog.Logger,
	newDecoderFn func(io.ReadCloser) Decoder,
) *tokenReader {
	return &tokenReader{
		readersCh:    readersCh,
		newDecoderFn: newDecoderFn,
		logger:       logger,
	}
}

func (tr *tokenReader) Read() (*models.Token, error) {
	var currentReader io.Closer

	for {
		if tr.decoder != nil {
			token, err := tr.decoder.NextToken()
			switch err {
			case nil:
				return token, nil
			case io.EOF:
				// Current decoder has finished, close the current reader
				if currentReader != nil {
					_ = currentReader.Close()
				}

				tr.decoder = nil
				currentReader = nil
			default:
				return nil, err
			}
		}

		if tr.decoder == nil {
			// We need a new decoder
			reader, ok := <-tr.readersCh
			if !ok {
				// Channel is closed, return EOF
				return nil, io.EOF
			}

			// Assign the new reader
			currentReader = reader
			tr.decoder = tr.newDecoderFn(reader)
		}
	}
}

// Close satisfies the DataReader interface
// but is a no-op for the tokenReader.
func (tr *tokenReader) Close() {
	tr.logger.Debug("closed token reader")
}
