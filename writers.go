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
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/aerospike/backup-go/encoding"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"github.com/google/uuid"
)

// **** Token Stats Writer ****

// statsSetterToken is an interface for setting the stats of a backup job
//
//go:generate mockery --name statsSetterToken --inpackage --exported=false
type statsSetterToken interface {
	AddUDFs(uint32)
	AddSIndexes(uint32)
	AddTotalBytesWritten(uint64)
}

type tokenStatsWriter struct {
	writer pipeline.DataWriter[*models.Token]
	stats  statsSetterToken
	logger *slog.Logger
}

func newWriterWithTokenStats(writer pipeline.DataWriter[*models.Token],
	stats statsSetterToken, logger *slog.Logger) *tokenStatsWriter {
	id := uuid.NewString()
	logger = logging.WithWriter(logger, id, logging.WriterTypeTokenStats)
	logger.Debug("created new token stats writer")

	return &tokenStatsWriter{
		writer: writer,
		stats:  stats,
		logger: logger,
	}
}

func (tw *tokenStatsWriter) Write(data *models.Token) (int, error) {
	n, err := tw.writer.Write(data)
	if err != nil {
		return 0, err
	}

	switch data.Type {
	case models.TokenTypeRecord:
	case models.TokenTypeUDF:
		tw.stats.AddUDFs(1)
	case models.TokenTypeSIndex:
		tw.stats.AddSIndexes(1)
	case models.TokenTypeInvalid:
		return 0, errors.New("invalid token")
	}

	tw.stats.AddTotalBytesWritten(uint64(n))

	return n, nil
}

func (tw *tokenStatsWriter) Close() error {
	tw.logger.Debug("closed token stats writer")
	return tw.writer.Close()
}

// **** Token Writer ****

// tokenWriter satisfies the DataWriter interface
// It writes the types from the models package as encoded data
// to an io.Writer. It uses an Encoder to encode the data.
type tokenWriter struct {
	encoder encoding.Encoder
	output  io.Writer
	logger  *slog.Logger
}

// newTokenWriter creates a new tokenWriter
func newTokenWriter(encoder encoding.Encoder, output io.Writer, logger *slog.Logger) *tokenWriter {
	id := uuid.NewString()
	logger = logging.WithWriter(logger, id, logging.WriterTypeToken)
	logger.Debug("created new token writer")

	return &tokenWriter{
		encoder: encoder,
		output:  output,
		logger:  logger,
	}
}

// Write encodes v and writes it to the output
func (w *tokenWriter) Write(v *models.Token) (int, error) {
	data, err := w.encoder.EncodeToken(v)
	if err != nil {
		return 0, fmt.Errorf("error encoding token: %w", err)
	}

	return w.output.Write(data)
}

// Close satisfies the DataWriter interface
// but is a no-op for the tokenWriter
func (w *tokenWriter) Close() error {
	w.logger.Debug("closed token writer")
	return nil
}
