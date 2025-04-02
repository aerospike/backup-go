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

package aerospike

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	a "github.com/aerospike/aerospike-client-go/v8"
	atypes "github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
)

type recordWriter interface {
	writeRecord(record *models.Record) error
	close() error
}

// RestoreWriter satisfies the DataWriter interface.
// It writes the types from the models package to an Aerospike client
// It is used to restore data from a backup.
type RestoreWriter[T models.TokenConstraint] struct {
	sindexWriter
	udfWriter
	recordWriter
	payloadWriter
	logger *slog.Logger
}

// NewRestoreWriter creates a new RestoreWriter.
func NewRestoreWriter[T models.TokenConstraint](
	asc dbWriter,
	writePolicy *a.WritePolicy,
	stats *models.RestoreStats,
	logger *slog.Logger,
	useBatchWrites bool,
	batchSize int,
	retryPolicy *models.RetryPolicy,
	ignoreRecordError bool,
) *RestoreWriter[T] {
	logger = logging.WithWriter(logger, uuid.NewString(), logging.WriterTypeRestore)
	logger.Debug("created new restore writer",
		slog.Bool("useBatchWrites", useBatchWrites),
		slog.Int("batchSize", batchSize),
		slog.Any("retryPolicy", *retryPolicy),
		slog.Bool("ignoreRecordError", ignoreRecordError),
	)

	return &RestoreWriter[T]{
		sindexWriter: sindexWriter{
			asc:         asc,
			writePolicy: writePolicy,
			logger:      logger,
		},
		udfWriter: udfWriter{
			asc:         asc,
			writePolicy: writePolicy,
			logger:      logger,
		},
		recordWriter: newRecordWriter(
			asc,
			writePolicy,
			stats,
			logger,
			useBatchWrites,
			batchSize,
			retryPolicy,
			ignoreRecordError,
		),
		payloadWriter: payloadWriter{
			asc,
			writePolicy,
			stats,
			retryPolicy,
			ignoreRecordError,
		},
		logger: logger,
	}
}

func newRecordWriter(
	asc dbWriter,
	writePolicy *a.WritePolicy,
	stats *models.RestoreStats,
	logger *slog.Logger,
	useBatchWrites bool,
	batchSize int,
	retryPolicy *models.RetryPolicy,
	ignoreRecordError bool,
) recordWriter {
	if useBatchWrites {
		return newBatchRecordWriter(
			asc,
			writePolicy,
			stats,
			retryPolicy,
			batchSize,
			ignoreRecordError,
			logger,
		)
	}

	return newSingleRecordWriter(
		asc,
		writePolicy,
		stats,
		retryPolicy,
		ignoreRecordError,
	)
}

// Write writes the types from the models package to an Aerospike DB.
func (rw *RestoreWriter[T]) Write(data T) (int, error) {
	switch v := any(data).(type) {
	case *models.ASBXToken:
		// Logic for ASBXToken
		return rw.writeASBXToken(v)
	case *models.Token:
		return rw.writeToken(v)
	default:
		return 0, fmt.Errorf("unsupported type: %T", data)
	}
}

func (rw *RestoreWriter[T]) writeToken(token *models.Token) (int, error) {
	switch token.Type {
	case models.TokenTypeRecord:
		return int(token.Size), rw.writeRecord(token.Record)
	case models.TokenTypeUDF:
		return int(token.Size), rw.writeUDF(token.UDF)
	case models.TokenTypeSIndex:
		return int(token.Size), rw.writeSecondaryIndex(token.SIndex)
	case models.TokenTypeInvalid:
		return 0, errors.New("invalid token")
	default:
		return 0, errors.New("unsupported token type")
	}
}

func (rw *RestoreWriter[T]) writeASBXToken(token *models.ASBXToken) (int, error) {
	return len(token.Payload), rw.writePayload(token)
}

// Close satisfies the DataWriter interface.
func (rw *RestoreWriter[T]) Close() error {
	rw.logger.Debug("close restore writer")
	return rw.close()
}

func attemptsLeft(rc *models.RetryPolicy, attempt uint) bool {
	if rc == nil {
		return attempt == 0 // only pass on 1st try.
	}

	return attempt <= rc.MaxRetries
}

func sleep(rc *models.RetryPolicy, attempt uint) {
	if rc == nil {
		return
	}

	duration := time.Duration(float64(rc.BaseTimeout) * math.Pow(rc.Multiplier, float64(attempt)))
	time.Sleep(duration)
}

func isNilOrAcceptableError(err a.Error) bool {
	return err == nil || err.Matches(atypes.GENERATION_ERROR, atypes.KEY_EXISTS_ERROR)
}

func shouldRetry(err a.Error) bool {
	return err != nil && err.Matches(
		atypes.NO_AVAILABLE_CONNECTIONS_TO_NODE,
		atypes.TIMEOUT,
		atypes.DEVICE_OVERLOAD,
		atypes.NETWORK_ERROR,
		atypes.SERVER_NOT_AVAILABLE,
		atypes.BATCH_FAILED,
		atypes.MAX_ERROR_RATE,
	)
}

func shouldIgnore(err a.Error) bool {
	return err != nil && err.Matches(
		atypes.RECORD_TOO_BIG,
		atypes.KEY_MISMATCH,
		atypes.BIN_NAME_TOO_LONG,
		atypes.ALWAYS_FORBIDDEN,
		atypes.FAIL_FORBIDDEN,
		atypes.BIN_TYPE_ERROR,
		atypes.BIN_NOT_FOUND,
		atypes.KEY_NOT_FOUND_ERROR,
	)
}
