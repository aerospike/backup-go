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
	"log/slog"
	"testing"

	"github.com/aerospike/backup-go/io/storage/aws/s3/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

	s3Mock := mocks.NewMocks3getter(t)
	ctx := context.Background()
	policy := models.NewDefaultRetryPolicy()
	logger := slog.Default()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

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
