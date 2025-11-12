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
	"context"
	"errors"
	"fmt"
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v8"
	atypes "github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/models"
)

type batchRecordWriter struct {
	ctx               context.Context
	asc               dbWriter
	writePolicy       *a.WritePolicy
	batchPolicy       *a.BatchPolicy
	stats             *models.RestoreStats
	logger            *slog.Logger
	retryPolicy       *models.RetryPolicy
	operationBuffer   []a.BatchRecordIfc
	rpsCollector      *metrics.Collector
	batchSize         int
	ignoreRecordError bool
}

func newBatchRecordWriter(
	ctx context.Context,
	asc dbWriter,
	writePolicy *a.WritePolicy,
	stats *models.RestoreStats,
	retryPolicy *models.RetryPolicy,
	rpsCollector *metrics.Collector,
	batchSize int,
	ignoreRecordError bool,
	logger *slog.Logger,
) *batchRecordWriter {
	if retryPolicy == nil {
		retryPolicy = models.NewDefaultRetryPolicy()
	}

	return &batchRecordWriter{
		ctx:               ctx,
		asc:               asc,
		writePolicy:       writePolicy,
		batchPolicy:       mapWriteToBatchPolicy(writePolicy),
		rpsCollector:      rpsCollector,
		stats:             stats,
		logger:            logger,
		retryPolicy:       retryPolicy,
		batchSize:         batchSize,
		ignoreRecordError: ignoreRecordError,
	}
}

func (rw *batchRecordWriter) writeRecord(record *models.Record) error {
	writeOp := rw.batchWrite(record)
	rw.operationBuffer = append(rw.operationBuffer, writeOp)

	rw.rpsCollector.Increment()

	if len(rw.operationBuffer) >= rw.batchSize {
		return rw.flushBuffer()
	}

	return nil
}

func (rw *batchRecordWriter) batchWrite(record *models.Record) *a.BatchWrite {
	policy := batchWritePolicy(rw.writePolicy, record)
	operations := putBinsOperations(record.Bins)

	return a.NewBatchWrite(policy, record.Key, operations...)
}

func batchWritePolicy(writePolicy *a.WritePolicy, r *models.Record) *a.BatchWritePolicy {
	policy := a.NewBatchWritePolicy()
	policy.SendKey = writePolicy.SendKey
	policy.RecordExistsAction = writePolicy.RecordExistsAction
	policy.Expiration = r.Expiration

	if writePolicy.GenerationPolicy == a.EXPECT_GEN_GT {
		policy.GenerationPolicy = a.EXPECT_GEN_GT
		policy.Generation = r.Generation
	}

	return policy
}

func putBinsOperations(bins a.BinMap) []*a.Operation {
	ops := make([]*a.Operation, 0, len(bins))
	for k, v := range bins {
		ops = append(ops, a.PutOp(a.NewBin(k, v)))
	}

	return ops
}

func (rw *batchRecordWriter) close() error {
	return rw.flushBuffer()
}

func (rw *batchRecordWriter) flushBuffer() error {
	if len(rw.operationBuffer) == 0 {
		rw.logger.Debug("Flush empty buffer")
		return nil
	}

	rw.logger.Debug("Starting batch operation",
		slog.Int("bufferSize", len(rw.operationBuffer)),
		slog.Any("retryPolicy", rw.retryPolicy),
	)

	var opErr error

	return rw.retryPolicy.Do(rw.ctx, func() error {
		rw.logger.Debug("Attempting batch operation",
			slog.Int("bufferSize", len(rw.operationBuffer)),
		)

		aerr := rw.asc.BatchOperate(rw.batchPolicy, rw.operationBuffer)

		if aerr != nil && aerr.IsInDoubt() {
			rw.stats.IncrErrorsInDoubt()
		}

		switch {
		case isNilOrAcceptableError(aerr),
			rw.ignoreRecordError && shouldIgnore(aerr):
			rw.operationBuffer, opErr = rw.processAndFilterOperations()

			if len(rw.operationBuffer) == 0 {
				rw.logger.Debug("All operations succeeded")
				return nil
			}

			rw.logger.Debug("Not all operations succeeded",
				slog.Int("remainingOperations", len(rw.operationBuffer)),
				slog.Any("error", opErr),
			)

			return opErr
		case shouldRetry(aerr):
			rw.logger.Debug("Retryable error occurred",
				slog.Any("error", aerr),
				slog.Int("remainingOperations", len(rw.operationBuffer)),
			)
			rw.stats.IncrPolicyRetries()

			return aerr
		default:
			// The default case is used for unexpected error.
			rw.stats.IncrPolicyRetries()

			return fmt.Errorf("%d operations failed: %w",
				len(rw.operationBuffer), errors.Join(aerr, opErr))
		}
	})
}

func (rw *batchRecordWriter) processAndFilterOperations() ([]a.BatchRecordIfc, error) {
	failedOps := make([]a.BatchRecordIfc, 0)

	errMap := make(map[atypes.ResultCode]error)

	for _, op := range rw.operationBuffer {
		if rw.processOperationResult(op) {
			errMap[op.BatchRec().ResultCode] = op.BatchRec().Err

			failedOps = append(failedOps, op)
		}
	}

	return failedOps, errMapToErr(errMap)
}

// processOperationResult increases statistics counters.
// it returns true if operation should be retried.
func (rw *batchRecordWriter) processOperationResult(op a.BatchRecordIfc) bool {
	code := op.BatchRec().ResultCode
	switch code {
	case atypes.RECORD_TOO_BIG,
		atypes.KEY_MISMATCH,
		atypes.BIN_NAME_TOO_LONG,
		atypes.ALWAYS_FORBIDDEN,
		atypes.FAIL_FORBIDDEN,
		atypes.BIN_TYPE_ERROR,
		atypes.BIN_NOT_FOUND:
		rw.stats.IncrRecordsIgnored()
		return false
	case atypes.OK:
		rw.stats.IncrRecordsInserted()
		return false
	case atypes.GENERATION_ERROR:
		rw.stats.IncrRecordsFresher()
		return false
	case atypes.KEY_EXISTS_ERROR:
		rw.stats.IncrRecordsExisted()
		return false
	default:
		return true
	}
}

func errMapToErr(errMap map[atypes.ResultCode]error) error {
	if len(errMap) == 0 {
		return nil
	}

	var result error

	for _, e := range errMap {
		if !errors.Is(result, e) {
			result = errors.Join(result, e)
		}
	}

	return result
}

func mapWriteToBatchPolicy(w *a.WritePolicy) *a.BatchPolicy {
	bp := a.NewBatchPolicy()
	bp.SocketTimeout = w.SocketTimeout
	bp.TotalTimeout = w.TotalTimeout

	return bp
}
