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
	"log/slog"

	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
)

// decoder is an interface for reading backup data as tokens.
// It is used to support different data formats.
// While the return type is `any`, the actual types returned should
// only be the types exposed by the models package.
// e.g. *models.Record, *models.UDF and *models.SecondaryIndex
//
//go:generate mockery --name Decoder
type decoder interface {
	NextToken() (*models.Token, error)
}

// tokenReader satisfies the DataReader interface
// It reads data as tokens using a decoder
type tokenReader struct {
	decoder decoder
	logger  *slog.Logger
}

// newTokenReader creates a new GenericReader
func newTokenReader(decoder decoder, logger *slog.Logger) *tokenReader {
	id := uuid.NewString()
	logger = logging.WithReader(logger, id, logging.ReaderTypeToken)
	logger.Debug("created new token reader")

	return &tokenReader{
		decoder: decoder,
		logger:  logger,
	}
}

// Read reads the next token from the decoder
func (dr *tokenReader) Read() (*models.Token, error) {
	return dr.decoder.NextToken()
}

// Close satisfies the DataReader interface
// but is a no-op for the tokenReader
func (dr *tokenReader) Close() {
	dr.logger.Debug("closed token reader")
}
