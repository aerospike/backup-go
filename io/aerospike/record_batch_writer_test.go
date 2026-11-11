// Copyright 2024-2026 Aerospike, Inc.
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
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	atypes "github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testAttempts is the total number of attempts for tests that exercise retries.
const testAttempts = 3

func asErr(rc atypes.ResultCode, inDoubt bool) a.Error {
	return &a.AerospikeError{
		ResultCode: rc,
		InDoubt:    inDoubt,
	}
}

type mockDBWriter struct {
	batchFn func(policy *a.BatchPolicy, records []a.BatchRecordIfc) a.Error
}

func (f *mockDBWriter) BatchOperate(p *a.BatchPolicy, recs []a.BatchRecordIfc) a.Error {
	if f.batchFn != nil {
		return f.batchFn(p, recs)
	}
	return nil
}

func (f *mockDBWriter) Put(*a.WritePolicy, *a.Key, a.BinMap) a.Error { return nil }
func (f *mockDBWriter) CreateComplexIndex(*a.WritePolicy, string, string, string, string, a.IndexType, a.IndexCollectionType, ...*a.CDTContext) (*a.IndexTask, a.Error) {
	return nil, nil
}
func (f *mockDBWriter) CreateIndexWithExpression(*a.WritePolicy, string, string, string, a.IndexType, a.IndexCollectionType, *a.Expression) (*a.IndexTask, a.Error) {
	return nil, nil
}
func (f *mockDBWriter) DropIndex(*a.WritePolicy, string, string, string) a.Error { return nil }
func (f *mockDBWriter) RegisterUDF(*a.WritePolicy, []byte, string, a.Language) (*a.RegisterTask, a.Error) {
	return nil, nil
}
func (f *mockDBWriter) PutPayload(*a.WritePolicy, *a.Key, []byte) a.Error { return nil }

func newTestWriter(t *testing.T, asc dbWriter) *batchRecordWriter {
	t.Helper()

	ctx := t.Context()
	wp := &a.WritePolicy{}
	stats := &models.RestoreStats{}
	logger := slog.New(slog.DiscardHandler)

	// One attempt, no sleep.
	rp := models.NewRetryPolicy(0, 1, 1)

	return newBatchRecordWriter(
		ctx, asc, wp, stats, rp, nil,
		100, false, logger,
	)
}

func mkOp(t *testing.T, rc atypes.ResultCode, err a.Error) a.BatchRecordIfc {
	t.Helper()
	key, e := a.NewKey("ns", "set", "k")
	if e != nil {
		t.Fatalf("unexpected key error: %v", e)
	}
	bw := a.NewBatchWrite(&a.BatchWritePolicy{}, key, a.PutOp(a.NewBin("b", 1)))
	bw.BatchRec().ResultCode = rc
	bw.BatchRec().Err = err
	return bw
}

func TestFlushBuffer_EmptyBuffer_NoOp(t *testing.T) {
	asc := &mockDBWriter{batchFn: func(_ *a.BatchPolicy, _ []a.BatchRecordIfc) a.Error {
		t.Fatal("BatchOperate must not be called for empty buffer")
		return nil
	}}
	rw := newTestWriter(t, asc)

	// empty buffer
	rw.operationBuffer = nil

	if err := rw.flushBuffer(); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestFlushBuffer_AllSuccess_WithNilBatchError(t *testing.T) {
	asc := &mockDBWriter{batchFn: func(_ *a.BatchPolicy, _ []a.BatchRecordIfc) a.Error {
		return nil
	}}
	rw := newTestWriter(t, asc)

	rw.operationBuffer = []a.BatchRecordIfc{
		// inserted
		mkOp(t, atypes.OK, nil),
		// existed
		mkOp(t, atypes.KEY_EXISTS_ERROR, nil),
		// fresher
		mkOp(t, atypes.GENERATION_ERROR, nil),
	}

	if err := rw.flushBuffer(); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got := len(rw.operationBuffer); got != 0 {
		t.Fatalf("expected buffer emptied, got %d remaining ops", got)
	}
}

func TestFlushBuffer_AllSuccess_WithAcceptableBatchError(t *testing.T) {
	asc := &mockDBWriter{batchFn: func(_ *a.BatchPolicy, _ []a.BatchRecordIfc) a.Error {
		return asErr(atypes.KEY_EXISTS_ERROR, false)
	}}
	rw := newTestWriter(t, asc)

	rw.operationBuffer = []a.BatchRecordIfc{
		mkOp(t, atypes.OK, nil),
		mkOp(t, atypes.GENERATION_ERROR, nil),
	}

	if err := rw.flushBuffer(); err != nil {
		t.Fatalf("expected nil error with acceptable batch error, got %v", err)
	}
	if got := len(rw.operationBuffer); got != 0 {
		t.Fatalf("expected buffer emptied, got %d remaining ops", got)
	}
}

func TestFlushBuffer_PartialFailures_WithNilBatchError_ReturnsOpErr(t *testing.T) {
	asc := &mockDBWriter{batchFn: func(_ *a.BatchPolicy, _ []a.BatchRecordIfc) a.Error {
		return nil
	}}
	rw := newTestWriter(t, asc)

	retryCause := asErr(atypes.TIMEOUT, false)
	rw.operationBuffer = []a.BatchRecordIfc{
		mkOp(t, atypes.OK, nil),
		mkOp(t, atypes.TIMEOUT, retryCause),
	}

	err := rw.flushBuffer()
	if err == nil {
		t.Fatalf("expected non-nil error due to remaining failed ops")
	}

	if got := len(rw.operationBuffer); got != 1 {
		t.Fatalf("expected 1 remaining op to retry, got %d", got)
	}

	var ae a.Error
	if !errors.As(err, &ae) || !ae.Matches(atypes.TIMEOUT) {
		t.Fatalf("expected returned error to contain TIMEOUT, got %v", err)
	}
}

func TestFlushBuffer_RetryableBatchError_ShouldRetryBranch(t *testing.T) {
	retryErr := asErr(atypes.TIMEOUT, true)
	callCount := 0

	asc := &mockDBWriter{batchFn: func(_ *a.BatchPolicy, _ []a.BatchRecordIfc) a.Error {
		callCount++
		return retryErr
	}}
	rw := newTestWriter(t, asc)

	rw.operationBuffer = []a.BatchRecordIfc{mkOp(t, atypes.OK, nil)}

	err := rw.flushBuffer()
	if err == nil {
		t.Fatalf("expected non-nil error from retryable batch error")
	}
	if callCount == 0 {
		t.Fatalf("expected BatchOperate to be called at least once")
	}

	if got := len(rw.operationBuffer); got != 1 {
		t.Fatalf("expected buffer unchanged, got %d ops", got)
	}

	var ae a.Error
	if !errors.As(err, &ae) || !ae.Matches(atypes.TIMEOUT) {
		t.Fatalf("expected TIMEOUT aerospike error, got %v", err)
	}
}

func TestFlushBuffer_NonRetryableBatchError_DefaultBranch(t *testing.T) {
	nonRetry := asErr(atypes.PARAMETER_ERROR, false)

	asc := &mockDBWriter{batchFn: func(_ *a.BatchPolicy, _ []a.BatchRecordIfc) a.Error {
		return nonRetry
	}}
	rw := newTestWriter(t, asc)

	rw.operationBuffer = []a.BatchRecordIfc{
		mkOp(t, atypes.OK, nil),
		mkOp(t, atypes.OK, nil),
	}

	err := rw.flushBuffer()
	if err == nil {
		t.Fatalf("expected error on default branch")
	}

	var ae a.Error
	if !errors.As(err, &ae) || !ae.Matches(atypes.PARAMETER_ERROR) {
		t.Fatalf("expected PARAMETER_ERROR aerospike error, got %v", err)
	}
	if got := len(rw.operationBuffer); got != 2 {
		t.Fatalf("expected buffer unchanged, got %d ops", got)
	}
}

func TestFlushBuffer_IgnoreRecordError_Path(t *testing.T) {
	ignorableBatch := asErr(atypes.KEY_NOT_FOUND_ERROR, false)

	asc := &mockDBWriter{batchFn: func(_ *a.BatchPolicy, _ []a.BatchRecordIfc) a.Error { return ignorableBatch }}
	rw := newTestWriter(t, asc)
	rw.ignoreRecordError = true

	rw.operationBuffer = []a.BatchRecordIfc{
		mkOp(t, atypes.OK, nil),
		mkOp(t, atypes.TIMEOUT, asErr(atypes.TIMEOUT, false)),
	}

	err := rw.flushBuffer()
	if err == nil {
		t.Fatalf("expected non-nil opErr because one op should remain")
	}

	if got := len(rw.operationBuffer); got != 1 {
		t.Fatalf("expected 1 remaining op, got %d", got)
	}
}

func TestFlushBuffer_RejectedRecord(t *testing.T) {
	t.Parallel()

	rejectCodes := []atypes.ResultCode{
		atypes.RECORD_TOO_BIG,
		atypes.KEY_MISMATCH,
		atypes.BIN_NAME_TOO_LONG,
		atypes.ALWAYS_FORBIDDEN,
		atypes.FAIL_FORBIDDEN,
		atypes.BIN_TYPE_ERROR,
		atypes.BIN_NOT_FOUND,
	}

	tests := []struct {
		name              string
		ignoreRecordError bool
		wantErr           bool
		wantRemaining     int
		wantIgnored       uint64
		wantCalls         int
	}{
		{
			name:          "retried and fails without ignore record error",
			wantErr:       true,
			wantRemaining: 1,
			wantCalls:     testAttempts,
		},
		{
			name:              "ignored with ignore record error",
			ignoreRecordError: true,
			wantIgnored:       1,
			wantCalls:         1,
		},
	}

	for _, code := range rejectCodes {
		for _, tt := range tests {
			t.Run(code.String()+"/"+tt.name, func(t *testing.T) {
				t.Parallel()

				var calls int

				asc := &mockDBWriter{batchFn: func(_ *a.BatchPolicy, _ []a.BatchRecordIfc) a.Error {
					calls++
					return nil
				}}
				rw := newTestWriter(t, asc)
				rw.ignoreRecordError = tt.ignoreRecordError
				// Several attempts, no sleep.
				rw.retryPolicy = models.NewRetryPolicy(0, 1, testAttempts)

				rw.operationBuffer = []a.BatchRecordIfc{
					mkOp(t, atypes.OK, nil),
					mkOp(t, code, asErr(code, false)),
				}

				err := rw.flushBuffer()
				if tt.wantErr {
					var ae a.Error
					require.ErrorAs(t, err, &ae)
					assert.True(t, ae.Matches(code))
				} else {
					require.NoError(t, err)
				}

				assert.Equal(t, tt.wantCalls, calls)
				assert.Len(t, rw.operationBuffer, tt.wantRemaining)
				assert.Equal(t, tt.wantIgnored, rw.stats.GetRecordsIgnored())
				assert.Equal(t, uint64(1), rw.stats.GetRecordsInserted())
			})
		}
	}
}

func TestFlushBuffer_RetryResendsOnlyFailedOps(t *testing.T) {
	t.Parallel()

	var sentPerCall []int

	asc := &mockDBWriter{batchFn: func(_ *a.BatchPolicy, recs []a.BatchRecordIfc) a.Error {
		sentPerCall = append(sentPerCall, len(recs))

		// The retried operation succeeds on the second attempt.
		if len(sentPerCall) > 1 {
			for _, r := range recs {
				r.BatchRec().ResultCode = atypes.OK
				r.BatchRec().Err = nil
			}
		}

		return nil
	}}
	rw := newTestWriter(t, asc)
	rw.retryPolicy = models.NewRetryPolicy(0, 1, testAttempts)

	rw.operationBuffer = []a.BatchRecordIfc{
		mkOp(t, atypes.OK, nil),
		mkOp(t, atypes.TIMEOUT, asErr(atypes.TIMEOUT, false)),
	}

	require.NoError(t, rw.flushBuffer())

	assert.Equal(t, []int{2, 1}, sentPerCall)
	assert.Empty(t, rw.operationBuffer)
	assert.Equal(t, uint64(2), rw.stats.GetRecordsInserted())
}
