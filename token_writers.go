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
	"fmt"
	"io"
	"log/slog"

	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe"
	"github.com/google/uuid"
)

// statsSetterToken is an interface for setting the stats of a backup job.
//
//go:generate mockery --name statsSetterToken --inpackage --exported=false
type statsSetterToken interface {
	AddUDFs(uint32)
	AddSIndexes(uint32)
}

type tokenStatsWriter[T models.TokenConstraint] struct {
	writer pipe.Writer[T]
	stats  statsSetterToken
	logger *slog.Logger
}

func newWriterWithTokenStats[T models.TokenConstraint](
	writer pipe.Writer[T],
	stats statsSetterToken,
	logger *slog.Logger,
) *tokenStatsWriter[T] {
	id := uuid.NewString()
	logger = logging.WithWriter(logger, id, logging.WriterTypeTokenStats)
	logger.Debug("created new token stats writer")

	return &tokenStatsWriter[T]{
		writer: writer,
		stats:  stats,
		logger: logger,
	}
}

func (tw *tokenStatsWriter[T]) Write(ctx context.Context, data T) (int, error) {
	n, err := tw.writer.Write(ctx, data)
	if err != nil {
		return 0, err
	}

	// We set stats only for ASB Tokens at the moment.
	t, ok := any(data).(*models.Token)
	if ok {
		switch t.Type {
		case models.TokenTypeRecord:
		case models.TokenTypeUDF:
			tw.stats.AddUDFs(1)
		case models.TokenTypeSIndex:
			tw.stats.AddSIndexes(1)
		case models.TokenTypeInvalid:
			return 0, errors.New("invalid token")
		}
	}

	return n, nil
}

func (tw *tokenStatsWriter[T]) Close() error {
	tw.logger.Debug("closed token stats writer")
	return tw.writer.Close()
}

// tokenWriter satisfies the DataWriter interface.
// It writes the types from the models package as encoded data
// to an io.Writer. It uses an Encoder to encode the data.
type tokenWriter[T models.TokenConstraint] struct {
	encoder   Encoder[T]
	output    io.Writer
	logger    *slog.Logger
	stateInfo *stateInfo
}

type stateInfo struct {
	// Channel to send save signal.
	recordsStateChan chan<- models.PartitionFilterSerialized
	// Number of writer.
	n int
}

func newStateInfo(recordsStateChan chan<- models.PartitionFilterSerialized, n int) *stateInfo {
	return &stateInfo{
		recordsStateChan: recordsStateChan,
		n:                n,
	}
}

// newTokenWriter creates a new tokenWriter.
func newTokenWriter[T models.TokenConstraint](
	encoder Encoder[T],
	output io.Writer,
	logger *slog.Logger,
	stateInfo *stateInfo,
) *tokenWriter[T] {
	id := uuid.NewString()
	logger = logging.WithWriter(logger, id, logging.WriterTypeToken)
	logger.Debug("created new token writer")

	return &tokenWriter[T]{
		encoder:   encoder,
		output:    output,
		logger:    logger,
		stateInfo: stateInfo,
	}
}

// Write encodes v and writes it to the output.
func (w *tokenWriter[T]) Write(ctx context.Context, v T) (int, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	data, err := w.encoder.EncodeToken(v)
	if err != nil {
		return 0, fmt.Errorf("error encoding token: %w", err)
	}

	// We set state only for ASB Tokens at the moment.
	t, ok := any(v).(*models.Token)
	if ok {
		if w.stateInfo != nil && t.Filter != nil {
			// Set a worker number.
			t.Filter.N = w.stateInfo.n
			// Check context before process.
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case w.stateInfo.recordsStateChan <- *t.Filter:
			}
		}
	}

	return w.output.Write(data)
}

// Close satisfies the DataWriter interface
// but is a no-op for the tokenWriter.
func (w *tokenWriter[T]) Close() error {
	w.logger.Debug("closed token writer")
	return nil
}
