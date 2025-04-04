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
	"log/slog"
	"math"
	"time"

	a "github.com/aerospike/aerospike-client-go/v8"
	atypes "github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"github.com/google/uuid"
)

type recordWriter interface {
	writeRecord(record *models.Record) error
	close() error
}

// restoreWriter satisfies the DataWriter interface.
// It writes the types from the models package to an Aerospike client
// It is used to restore data from a backup.
type restoreWriter struct {
	sindexWriter
	udfWriter
	recordWriter
	logger *slog.Logger
}

// NewRestoreWriter creates a new restoreWriter.
func NewRestoreWriter(
	asc dbWriter,
	writePolicy *a.WritePolicy,
	stats *models.RestoreStats,
	logger *slog.Logger,
	useBatchWrites bool,
	batchSize int,
	retryPolicy *models.RetryPolicy,
	ignoreRecordError bool,
) pipeline.DataWriter[*models.Token] {
	logger = logging.WithWriter(logger, uuid.NewString(), logging.WriterTypeRestore)
	logger.Debug("created new restore writer")

	return &restoreWriter{
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
func (rw *restoreWriter) Write(data *models.Token) (int, error) {
	switch data.Type {
	case models.TokenTypeRecord:
		return int(data.Size), rw.writeRecord(data.Record)
	case models.TokenTypeUDF:
		return int(data.Size), rw.writeUDF(data.UDF)
	case models.TokenTypeSIndex:
		return int(data.Size), rw.writeSecondaryIndex(data.SIndex)
	case models.TokenTypeInvalid:
		return 0, errors.New("invalid token")
	default:
		return 0, errors.New("unsupported token type")
	}
}

// Close satisfies the DataWriter interface.
func (rw *restoreWriter) Close() error {
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
	)
}
