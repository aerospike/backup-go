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

package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"syscall"
	"testing"
	"time"

	"github.com/aerospike/backup-go/io/storage/internal/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	testSize     = int64(1024)
	testFileInfo = "bucket:file"
)

var (
	errTest       = errors.New("test error")
	errNonNetwork = errors.New("some other error")
)

func TestRetryableReader_New(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	policy := models.NewDefaultRetryPolicy()
	logger := slog.Default()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		readerMock := mocks.NewMockreaderCloser(t)

		rrMock := mocks.NewMockrangeReader(t)
		rrMock.On("OpenRange", mock.Anything, mock.Anything, mock.Anything).
			Return(readerMock, nil)
		rrMock.On("GetSize").
			Return(testSize)
		rrMock.On("GetInfo").
			Return(testFileInfo)

		rr, err := NewRetryableReader(
			ctx,
			rrMock,
			policy,
			logger,
		)
		require.NoError(t, err)
		require.NotNil(t, rr)
	})

	t.Run("Error get object", func(t *testing.T) {
		t.Parallel()

		rrMock := mocks.NewMockrangeReader(t)
		rrMock.On("OpenRange", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, errTest)
		rrMock.On("GetSize").
			Return(testSize)
		rrMock.On("GetInfo").
			Return(testFileInfo)

		rr, err := NewRetryableReader(
			ctx,
			rrMock,
			policy,
			logger,
		)
		require.ErrorIs(t, err, errTest)
		require.Nil(t, rr)
	})
}

func TestRetryableReader_Read(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	policy := models.NewDefaultRetryPolicy()
	logger := slog.Default()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		readerMock := mocks.NewMockreaderCloser(t)
		readerMock.On("Read", mock.Anything).Return(10, nil)
		readerMock.On("Close").Return(nil)

		rrMock := mocks.NewMockrangeReader(t)
		rrMock.On("OpenRange", mock.Anything, mock.Anything, mock.Anything).
			Return(readerMock, nil)
		rrMock.On("GetSize").
			Return(testSize)
		rrMock.On("GetInfo").
			Return(testFileInfo)

		rr, err := NewRetryableReader(
			ctx,
			rrMock,
			policy,
			logger,
		)
		require.NoError(t, err)
		defer rr.Close()

		buf := make([]byte, 10)
		n, err := rr.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 10, n)
		require.Equal(t, int64(10), rr.offset)
	})

	t.Run("Reader closed", func(t *testing.T) {
		t.Parallel()

		readerMock := mocks.NewMockreaderCloser(t)
		readerMock.On("Close").Return(nil)

		rrMock := mocks.NewMockrangeReader(t)
		rrMock.On("OpenRange", mock.Anything, mock.Anything, mock.Anything).
			Return(readerMock, nil)
		rrMock.On("GetSize").
			Return(testSize)
		rrMock.On("GetInfo").
			Return(testFileInfo)

		rr, err := NewRetryableReader(
			ctx,
			rrMock,
			policy,
			logger,
		)
		require.NoError(t, err)

		err = rr.Close()
		require.NoError(t, err)

		buf := make([]byte, 10)
		_, err = rr.Read(buf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "reader is closed")
	})

	t.Run("EOF when offset >= totalSize", func(t *testing.T) {
		t.Parallel()

		readerMock := mocks.NewMockreaderCloser(t)
		readerMock.On("Close").Return(nil)

		rrMock := mocks.NewMockrangeReader(t)
		rrMock.On("OpenRange", mock.Anything, mock.Anything, mock.Anything).
			Return(readerMock, nil)
		rrMock.On("GetSize").
			Return(testSize)
		rrMock.On("GetInfo").
			Return(testFileInfo)

		rr, err := NewRetryableReader(
			ctx,
			rrMock,
			policy,
			logger,
		)
		require.NoError(t, err)
		defer rr.Close()

		rr.offset = rr.totalSize

		buf := make([]byte, 10)
		n, err := rr.Read(buf)
		require.Equal(t, io.EOF, err)
		require.Equal(t, 0, n)
	})

	t.Run("Network error with successful retry", func(t *testing.T) {
		t.Parallel()

		readerErrorMock := mocks.NewMockreaderCloser(t)
		readerErrorMock.On("Read", mock.Anything).Return(0, syscall.ECONNRESET)
		readerErrorMock.On("Close").Return(nil)

		readerMock := mocks.NewMockreaderCloser(t)
		readerMock.On("Read", mock.Anything).Return(10, nil)
		readerMock.On("Close").Return(nil)

		rrMock := mocks.NewMockrangeReader(t)
		rrMock.On("OpenRange", mock.Anything, mock.Anything, mock.Anything).
			Return(readerErrorMock, nil).Once().
			On("OpenRange", mock.Anything, mock.Anything, mock.Anything).
			Return(readerMock, nil).Once()
		rrMock.On("GetSize").
			Return(testSize)
		rrMock.On("GetInfo").
			Return(testFileInfo)

		rr, err := NewRetryableReader(
			ctx,
			rrMock,
			policy,
			logger,
		)
		require.NoError(t, err)
		defer rr.Close()

		rr.offset = 5

		buf := make([]byte, 10)
		n, err := rr.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 10, n)
		require.Equal(t, int64(15), rr.offset)
	})

	t.Run("Network error - failed to reopen stream", func(t *testing.T) {
		t.Parallel()

		readerMock := mocks.NewMockreaderCloser(t)
		readerMock.EXPECT().Read(mock.Anything).Return(5, io.ErrUnexpectedEOF).Once().
			On("Read", mock.Anything).Return(5, nil)
		readerMock.On("Close").Return(nil)

		rrMock := mocks.NewMockrangeReader(t)
		// First call ok.
		rrMock.EXPECT().OpenRange(ctx, mock.Anything, mock.Anything).Return(readerMock, nil).Once().
			// Second call fails.
			On("OpenRange", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, errTest)
		rrMock.On("GetSize").
			Return(testSize)
		rrMock.On("GetInfo").
			Return(testFileInfo)

		rr, err := NewRetryableReader(
			ctx,
			rrMock,
			policy,
			logger,
		)
		require.NoError(t, err)
		defer rr.Close()

		buf := make([]byte, 2048)
		n, err := rr.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 5, n)
	})

	t.Run("Non-network error", func(t *testing.T) {
		t.Parallel()

		readerErrorMock := mocks.NewMockreaderCloser(t)
		readerErrorMock.On("Read", mock.Anything).Return(0, errNonNetwork)
		readerErrorMock.On("Close").Return(nil)

		rrMock := mocks.NewMockrangeReader(t)
		rrMock.On("OpenRange", mock.Anything, mock.Anything, mock.Anything).
			Return(readerErrorMock, nil)
		rrMock.On("GetSize").
			Return(testSize)
		rrMock.On("GetInfo").
			Return(testFileInfo)

		rr, err := NewRetryableReader(
			ctx,
			rrMock,
			policy,
			logger,
		)
		require.NoError(t, err)
		defer rr.Close()

		buf := make([]byte, 10)
		n, err := rr.Read(buf)
		require.ErrorIs(t, err, errNonNetwork)
		require.Equal(t, 0, n)
	})

	t.Run("Exceeded retry attempts", func(t *testing.T) {
		t.Parallel()

		limitedPolicy := &models.RetryPolicy{
			MaxRetries:  1,
			BaseTimeout: time.Millisecond,
		}

		readerErrorMock := mocks.NewMockreaderCloser(t)
		readerErrorMock.On("Read", mock.Anything).Return(0, syscall.ECONNREFUSED)
		readerErrorMock.On("Close").Return(nil)

		rrMock := mocks.NewMockrangeReader(t)
		rrMock.On("OpenRange", mock.Anything, mock.Anything, mock.Anything).
			Return(readerErrorMock, nil)
		rrMock.On("GetSize").
			Return(testSize)
		rrMock.On("GetInfo").
			Return(testFileInfo)

		rr, err := NewRetryableReader(
			ctx,
			rrMock,
			limitedPolicy,
			logger,
		)
		require.NoError(t, err)
		defer rr.Close()

		buf := make([]byte, 10)
		n, err := rr.Read(buf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed after")
		require.Contains(t, err.Error(), syscall.ECONNREFUSED.Error())
		require.Equal(t, 0, n)
	})
}

func TestIsNetworkErrorValid(t *testing.T) {
	t.Parallel()

	checkErrs := []error{
		syscall.ECONNRESET,   // "connection reset"
		syscall.EPIPE,        // "broken pipe"
		syscall.ETIMEDOUT,    // "timeout"
		syscall.ECONNREFUSED, // "connection refused"
		syscall.ENETUNREACH,  // "network is unreachable"
		syscall.ECONNABORTED, // "software caused connection abort"
		syscall.EHOSTUNREACH, // "no route to host"
		io.ErrUnexpectedEOF,
		io.ErrClosedPipe,
	}

	for _, e := range checkErrs {
		// wrap errors to emulate client.
		w1Err := fmt.Errorf("wrapped error: %w", e)
		w2Err := fmt.Errorf("wrapped error: %w", w1Err)
		w3Err := fmt.Errorf("wrapped error: %w", w2Err)

		t.Run(fmt.Sprintf("test: %s", e.Error()), func(t *testing.T) {
			t.Parallel()

			result := isNetworkError(w3Err)
			assert.Equal(t, true, result)
		})
	}
}

func TestIsNetworkErrorInvalid(t *testing.T) {
	t.Parallel()

	result := isNetworkError(errors.New("some other error"))
	assert.Equal(t, false, result)

	result = isNetworkError(nil)
	assert.Equal(t, false, result)
}
