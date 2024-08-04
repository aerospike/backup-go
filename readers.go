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
	"log/slog"

	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
)

// tokenReader satisfies the DataReader interface.
// It reads data as tokens using a Decoder.
type tokenReader struct {
	decoder Decoder
	logger  *slog.Logger
}

// newTokenReader creates a new tokenReader.
func newTokenReader(decoder Decoder, logger *slog.Logger) *tokenReader {
	id := uuid.NewString()
	logger = logging.WithReader(logger, id, logging.ReaderTypeToken)
	logger.Debug("created new token reader")

	return &tokenReader{
		decoder: decoder,
		logger:  logger,
	}
}

// Read reads the next token from the Decoder.
func (dr *tokenReader) Read() (*models.Token, error) {
	return dr.decoder.NextToken()
}

// Close satisfies the DataReader interface
// but is a no-op for the tokenReader.
func (dr *tokenReader) Close() {
	dr.logger.Debug("closed token reader")
}
