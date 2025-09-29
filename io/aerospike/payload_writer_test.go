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
	"log/slog"
	"testing"
	"time"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/io/aerospike/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/require"
)

func TestPayloadWriterSuccess(t *testing.T) {
	t.Parallel()

	key, aerr := a.NewKey(testNamespace, testSet, "key")
	if aerr != nil {
		panic(aerr)
	}

	// Create a payload with enough bytes for the header and info fields
	payload := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	token := &models.ASBXToken{
		Key:     key,
		Payload: payload,
	}

	policy := &a.WritePolicy{}
	mockDBWriter := mocks.NewMockdbWriter(t)
	mockDBWriter.EXPECT().PutPayload(policy, key, payload).Return(nil)

	stats := models.NewRestoreStats()
	rpsCollector := metrics.NewCollector(context.Background(), slog.Default(), metrics.RecordsPerSecond,
		"test metric message", true)

	writer := newPayloadWriter(
		mockDBWriter,
		policy,
		stats,
		nil,
		rpsCollector,
		false,
	)

	err := writer.writePayload(token)
	require.Nil(t, err)
	require.Equal(t, 1, int(stats.GetRecordsInserted()))

	mockDBWriter.AssertExpectations(t)
}

func TestPayloadWriterRetry(t *testing.T) {
	t.Parallel()

	key, aerr := a.NewKey(testNamespace, testSet, "key")
	if aerr != nil {
		panic(aerr)
	}

	// Create a payload with enough bytes for the header and info fields
	payload := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	token := &models.ASBXToken{
		Key:     key,
		Payload: payload,
	}

	policy := &a.WritePolicy{}
	mockDBWriter := mocks.NewMockdbWriter(t)

	// First call returns an error, second call succeeds
	mockDBWriter.EXPECT().PutPayload(policy, key, payload).
		Return(a.ErrTimeout).Once()
	mockDBWriter.EXPECT().PutPayload(policy, key, payload).
		Return(nil).Once()

	stats := models.NewRestoreStats()
	retryPolicy := &models.RetryPolicy{
		BaseTimeout: 10 * time.Millisecond,
		Multiplier:  1,
		MaxRetries:  2,
	}
	rpsCollector := metrics.NewCollector(context.Background(), slog.Default(), metrics.RecordsPerSecond,
		"test metric message", true)

	writer := newPayloadWriter(
		mockDBWriter,
		policy,
		stats,
		retryPolicy,
		rpsCollector,
		false,
	)

	err := writer.writePayload(token)
	require.Nil(t, err)
	require.Equal(t, 1, int(stats.GetRecordsInserted()))

	mockDBWriter.AssertExpectations(t)
}

func TestPayloadWriterRetryExhausted(t *testing.T) {
	t.Parallel()

	key, aerr := a.NewKey(testNamespace, testSet, "key")
	if aerr != nil {
		panic(aerr)
	}

	// Create a payload with enough bytes for the header and info fields
	payload := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	token := &models.ASBXToken{
		Key:     key,
		Payload: payload,
	}

	policy := &a.WritePolicy{}
	mockDBWriter := mocks.NewMockdbWriter(t)

	// All calls return an error
	mockDBWriter.EXPECT().PutPayload(policy, key, payload).
		Return(a.ErrTimeout).Times(3)

	stats := models.NewRestoreStats()
	retryPolicy := &models.RetryPolicy{
		BaseTimeout: 10 * time.Millisecond,
		Multiplier:  1,
		MaxRetries:  3,
	}
	rpsCollector := metrics.NewCollector(context.Background(), slog.Default(), metrics.RecordsPerSecond,
		"test metric message", true)

	writer := newPayloadWriter(
		mockDBWriter,
		policy,
		stats,
		retryPolicy,
		rpsCollector,
		false,
	)

	err := writer.writePayload(token)
	require.NotNil(t, err)
	require.Equal(t, 0, int(stats.GetRecordsInserted()))

	mockDBWriter.AssertExpectations(t)
}

func TestPayloadWriterNonRetryableError(t *testing.T) {
	t.Parallel()

	key, aerr := a.NewKey(testNamespace, testSet, "key")
	if aerr != nil {
		panic(aerr)
	}

	// Create a payload with enough bytes for the header and info fields
	payload := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	token := &models.ASBXToken{
		Key:     key,
		Payload: payload,
	}

	policy := &a.WritePolicy{}
	mockDBWriter := mocks.NewMockdbWriter(t)

	// Return a non-retryable error
	mockDBWriter.EXPECT().PutPayload(policy, key, payload).Return(a.ErrInvalidParam)

	stats := models.NewRestoreStats()
	retryPolicy := &models.RetryPolicy{
		BaseTimeout: 10 * time.Millisecond,
		Multiplier:  1,
		MaxRetries:  2,
	}
	rpsCollector := metrics.NewCollector(context.Background(), slog.Default(), metrics.RecordsPerSecond,
		"test metric message", true)

	writer := newPayloadWriter(
		mockDBWriter,
		policy,
		stats,
		retryPolicy,
		rpsCollector,
		false,
	)

	err := writer.writePayload(token)
	require.NotNil(t, err)
	require.Equal(t, 0, int(stats.GetRecordsInserted()))

	mockDBWriter.AssertExpectations(t)
}
