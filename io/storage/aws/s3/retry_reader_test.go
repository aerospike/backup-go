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

package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aerospike/backup-go/io/storage/aws/s3/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	testKey = "test-key"
)

var (
	errTest        = errors.New("test error")
	testHeadObject = &s3.HeadObjectOutput{
		ContentLength: aws.Int64(1024),
	}
	testObjectOutput = &s3.GetObjectOutput{
		Body: nil,
	}
)

func TestRetryableReader_New(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	policy := models.NewDefaultRetryPolicy()
	logger := slog.Default()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		s3Mock := mocks.NewMocks3getter(t)
		s3Mock.On("HeadObject", mock.Anything, mock.Anything).
			Return(testHeadObject, nil)
		s3Mock.On("GetObject", mock.Anything, mock.Anything).
			Return(testObjectOutput, nil)

		rr, err := newRetryableReader(
			ctx,
			s3Mock,
			policy,
			logger,
			testBucket,
			testKey,
		)
		require.NoError(t, err)
		require.NotNil(t, rr)
	})

	t.Run("Error head object", func(t *testing.T) {
		t.Parallel()

		s3Mock := mocks.NewMocks3getter(t)
		s3Mock.On("HeadObject", mock.Anything, mock.Anything).
			Return(nil, errTest)

		rr, err := newRetryableReader(
			ctx,
			s3Mock,
			policy,
			logger,
			testBucket,
			testKey,
		)
		require.ErrorIs(t, err, errTest)
		require.Nil(t, rr)
	})

	t.Run("Error get object", func(t *testing.T) {
		t.Parallel()

		s3Mock := mocks.NewMocks3getter(t)
		s3Mock.On("HeadObject", mock.Anything, mock.Anything).
			Return(testHeadObject, nil)
		s3Mock.On("GetObject", mock.Anything, mock.Anything).
			Return(nil, errTest)

		rr, err := newRetryableReader(
			ctx,
			s3Mock,
			policy,
			logger,
			testBucket,
			testKey,
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

		s3Mock := mocks.NewMocks3getter(t)
		mockReader := mocks.NewMockreaderCloser(t)

		s3Mock.On("HeadObject", mock.Anything, mock.Anything).
			Return(testHeadObject, nil)
		s3Mock.On("GetObject", mock.Anything, mock.Anything).
			Return(&s3.GetObjectOutput{Body: mockReader}, nil)

		mockReader.On("Read", mock.Anything).Return(10, nil)
		mockReader.On("Close").Return(nil)

		rr, err := newRetryableReader(ctx, s3Mock, policy, logger, testBucket, testKey)
		require.NoError(t, err)
		defer rr.Close()

		buf := make([]byte, 10)
		n, err := rr.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 10, n)
		require.Equal(t, int64(10), rr.position)
	})

	t.Run("Reader closed", func(t *testing.T) {
		t.Parallel()

		s3Mock := mocks.NewMocks3getter(t)
		mockReader := mocks.NewMockreaderCloser(t)

		s3Mock.On("HeadObject", mock.Anything, mock.Anything).
			Return(testHeadObject, nil)
		s3Mock.On("GetObject", mock.Anything, mock.Anything).
			Return(&s3.GetObjectOutput{Body: mockReader}, nil)

		mockReader.On("Close").Return(nil)

		rr, err := newRetryableReader(ctx, s3Mock, policy, logger, testBucket, testKey)
		require.NoError(t, err)

		err = rr.Close()
		require.NoError(t, err)

		buf := make([]byte, 10)
		_, err = rr.Read(buf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "reader is closed")
	})

	t.Run("EOF when position >= totalSize", func(t *testing.T) {
		t.Parallel()

		s3Mock := mocks.NewMocks3getter(t)
		mockReader := mocks.NewMockreaderCloser(t)

		s3Mock.On("HeadObject", mock.Anything, mock.Anything).
			Return(testHeadObject, nil)
		s3Mock.On("GetObject", mock.Anything, mock.Anything).
			Return(&s3.GetObjectOutput{Body: mockReader}, nil)

		mockReader.On("Close").Return(nil)

		rr, err := newRetryableReader(ctx, s3Mock, policy, logger, testBucket, testKey)
		require.NoError(t, err)
		defer rr.Close()

		rr.position = rr.totalSize

		buf := make([]byte, 10)
		n, err := rr.Read(buf)
		require.Equal(t, io.EOF, err)
		require.Equal(t, 0, n)
	})

	t.Run("Network error with successful retry", func(t *testing.T) {
		t.Parallel()

		s3Mock := mocks.NewMocks3getter(t)
		mockReader1 := mocks.NewMockreaderCloser(t)
		mockReader2 := mocks.NewMockreaderCloser(t)

		s3Mock.On("HeadObject", mock.Anything, mock.Anything).
			Return(testHeadObject, nil)

		s3Mock.On("GetObject", mock.Anything, mock.MatchedBy(func(input *s3.GetObjectInput) bool {
			return input.Range == nil
		})).Return(&s3.GetObjectOutput{Body: mockReader1}, nil)

		s3Mock.On("GetObject", mock.Anything, mock.MatchedBy(func(input *s3.GetObjectInput) bool {
			return input.Range != nil
		})).Return(&s3.GetObjectOutput{Body: mockReader2}, nil)

		mockReader1.On("Read", mock.Anything).Return(5, errors.New("connection reset"))
		mockReader1.On("Close").Return(nil)

		mockReader2.On("Read", mock.Anything).Return(10, nil)
		mockReader2.On("Close").Return(nil)

		rr, err := newRetryableReader(ctx, s3Mock, policy, logger, testBucket, testKey)
		require.NoError(t, err)
		defer rr.Close()

		rr.position = 5

		buf := make([]byte, 10)
		n, err := rr.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 10, n)
		require.Equal(t, int64(15), rr.position)
	})

	t.Run("Network error - failed to reopen stream", func(t *testing.T) {
		t.Parallel()

		s3Mock := mocks.NewMocks3getter(t)
		mockReader := mocks.NewMockreaderCloser(t)

		s3Mock.On("HeadObject", mock.Anything, mock.Anything).
			Return(testHeadObject, nil)
		// First call ok, second error.
		s3Mock.EXPECT().GetObject(ctx, mock.AnythingOfType("*s3.GetObjectInput")).
			Return(&s3.GetObjectOutput{Body: mockReader}, nil).Once().
			On("GetObject", ctx, mock.AnythingOfType("*s3.GetObjectInput")).
			Return(nil, errTest).Once()

		mockReader.On("Read", mock.Anything).Return(5, errors.New("i/o timeout"))
		mockReader.On("Close").Return(nil)

		rr, err := newRetryableReader(ctx, s3Mock, policy, logger, testBucket, testKey)
		require.NoError(t, err)
		defer rr.Close()

		buf := make([]byte, 2048)
		n, err := rr.Read(buf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to reopen stream")
		require.Equal(t, 5, n)
	})

	t.Run("Non-network error", func(t *testing.T) {
		t.Parallel()

		s3Mock := mocks.NewMocks3getter(t)
		mockReader := mocks.NewMockreaderCloser(t)

		s3Mock.On("HeadObject", mock.Anything, mock.Anything).
			Return(testHeadObject, nil)
		s3Mock.On("GetObject", mock.Anything, mock.Anything).
			Return(&s3.GetObjectOutput{Body: mockReader}, nil)

		nonNetworkErr := errors.New("some other error")
		mockReader.On("Read", mock.Anything).Return(5, nonNetworkErr).Once()
		mockReader.On("Close").Return(nil).Maybe()

		rr, err := newRetryableReader(ctx, s3Mock, policy, logger, testBucket, testKey)
		require.NoError(t, err)
		defer rr.Close()

		buf := make([]byte, 10)
		n, err := rr.Read(buf)
		require.ErrorIs(t, err, nonNetworkErr)
		require.Equal(t, 5, n)
	})

	t.Run("Exceeded retry attempts", func(t *testing.T) {
		t.Parallel()

		limitedPolicy := &models.RetryPolicy{
			MaxRetries:  1,
			BaseTimeout: time.Millisecond,
		}

		s3Mock := mocks.NewMocks3getter(t)
		mockReader1 := mocks.NewMockreaderCloser(t)

		s3Mock.On("HeadObject", mock.Anything, mock.Anything).
			Return(testHeadObject, nil)

		s3Mock.EXPECT().GetObject(ctx, mock.AnythingOfType("*s3.GetObjectInput")).
			Return(&s3.GetObjectOutput{Body: mockReader1}, nil)

		networkErr := errors.New("connection refused")

		mockReader1.On("Read", mock.Anything).Return(0, networkErr)
		mockReader1.On("Close").Return(nil)

		rr, err := newRetryableReader(ctx, s3Mock, limitedPolicy, logger, testBucket, testKey)
		require.NoError(t, err)
		defer rr.Close()

		buf := make([]byte, 10)
		n, err := rr.Read(buf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed after")
		require.Contains(t, err.Error(), networkErr.Error())
		require.Equal(t, 0, n)
	})
}

func TestIsNetworkErrorValid(t *testing.T) {
	t.Parallel()

	for _, e := range netErrors {
		t.Run(fmt.Sprintf("test: %s", e), func(t *testing.T) {
			t.Parallel()

			result := isNetworkError(errors.New(e))
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
