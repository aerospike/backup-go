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
	"fmt"
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v7"
	atypes "github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/aerospike/backup-go/models"
)

type batchRecordWriter struct {
	asc             dbWriter
	writePolicy     *a.WritePolicy
	stats           *models.RestoreStats
	logger          *slog.Logger
	retryPolicy     *models.RetryPolicy
	operationBuffer []a.BatchRecordIfc
	batchSize       int
}

func (rw *batchRecordWriter) writeRecord(record *models.Record) error {
	writeOp := rw.batchWrite(record)
	rw.operationBuffer = append(rw.operationBuffer, writeOp)

	if len(rw.operationBuffer) > rw.batchSize {
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
	policy.RecordExistsAction = writePolicy.RecordExistsAction

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
		return nil
	}

	var attempt int
	for attemptsLeft(rw.retryPolicy, attempt) {
		err := rw.asc.BatchOperate(nil, rw.operationBuffer)

		if isNilOrAcceptableError(err) {
			rw.operationBuffer = rw.processAndFilterOperations()
			if len(rw.operationBuffer) == 0 {
				return nil // All operations succeeded
			}
		} else if !shouldRetry(err) {
			return fmt.Errorf("non-retryable error on restore: %w", err)
		}

		sleep(rw.retryPolicy, attempt)

		attempt++
	}

	return fmt.Errorf("max retries reached, %d operations failed", len(rw.operationBuffer))
}

func (rw *batchRecordWriter) processAndFilterOperations() []a.BatchRecordIfc {
	failedOps := make([]a.BatchRecordIfc, 0)

	for _, op := range rw.operationBuffer {
		if rw.processOperationResult(op) {
			failedOps = append(failedOps, op)
		}
	}

	return failedOps
}

// processOperationResult increases statistics counters.
// return true if operation should be retried.
func (rw *batchRecordWriter) processOperationResult(op a.BatchRecordIfc) bool {
	code := op.BatchRec().ResultCode
	switch code {
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
		rw.logger.Info("Unexpected batch operation result code",
			slog.String("code", code.String()),
			slog.String("key", op.BatchRec().Key.String()))

		return true
	}
}
