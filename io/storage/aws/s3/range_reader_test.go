// Copyright 2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this rangeReader except in compliance with the License.
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
	"testing"

	"github.com/aerospike/backup-go/io/storage/aws/s3/mocks"
	closerMock "github.com/aerospike/backup-go/io/storage/internal/mocks"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

const (
	testKey         = "test-key"
	testETag        = "test-etag"
	testSize        = int64(12345)
	testRangeHeader = "bytes=0-1023"
)

var (
	errS3Test = errors.New("test error")
)

func TestNewRangeReader(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bucket := aws.String(testBucket)
	key := aws.String(testKey)
	etag := aws.String(testETag)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMocks3Getter(t)
		clientMock.On("HeadObject", ctx, &s3.HeadObjectInput{
			Bucket: bucket,
			Key:    key,
		}).Return(&s3.HeadObjectOutput{
			ContentLength: aws.Int64(testSize),
			ETag:          etag,
		}, nil)

		reader, err := newRangeReader(ctx, clientMock, bucket, key)

		require.NoError(t, err)
		require.NotNil(t, reader)
		require.Equal(t, testSize, reader.size)
		require.Equal(t, bucket, reader.bucket)
		require.Equal(t, key, reader.key)
		require.Equal(t, etag, reader.etag)
		require.Equal(t, clientMock, reader.client)
	})

	t.Run("Success with nil ContentLength", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMocks3Getter(t)
		clientMock.On("HeadObject", ctx, &s3.HeadObjectInput{
			Bucket: bucket,
			Key:    key,
		}).Return(&s3.HeadObjectOutput{
			ContentLength: nil,
			ETag:          etag,
		}, nil)

		reader, err := newRangeReader(ctx, clientMock, bucket, key)

		require.NoError(t, err)
		require.NotNil(t, reader)
		require.Equal(t, int64(0), reader.size)
		require.Equal(t, etag, reader.etag)
	})

	t.Run("Success with zero ContentLength", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMocks3Getter(t)
		clientMock.On("HeadObject", ctx, &s3.HeadObjectInput{
			Bucket: bucket,
			Key:    key,
		}).Return(&s3.HeadObjectOutput{
			ContentLength: aws.Int64(0),
			ETag:          etag,
		}, nil)

		reader, err := newRangeReader(ctx, clientMock, bucket, key)

		require.NoError(t, err)
		require.NotNil(t, reader)
		require.Equal(t, int64(0), reader.size)
	})

	t.Run("Success with nil ETag", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMocks3Getter(t)
		clientMock.On("HeadObject", ctx, &s3.HeadObjectInput{
			Bucket: bucket,
			Key:    key,
		}).Return(&s3.HeadObjectOutput{
			ContentLength: aws.Int64(testSize),
			ETag:          nil,
		}, nil)

		reader, err := newRangeReader(ctx, clientMock, bucket, key)

		require.NoError(t, err)
		require.NotNil(t, reader)
		require.Nil(t, reader.etag)
	})

	t.Run("Error HeadObject failed", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMocks3Getter(t)
		clientMock.On("HeadObject", ctx, &s3.HeadObjectInput{
			Bucket: bucket,
			Key:    key,
		}).Return(nil, errS3Test)

		reader, err := newRangeReader(ctx, clientMock, bucket, key)

		require.ErrorIs(t, err, errS3Test)
		require.Contains(t, err.Error(), "failed to get head")
		require.Contains(t, err.Error(), testKey)
		require.Nil(t, reader)
	})
}

func TestRangeReader_OpenRange(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bucket := aws.String(testBucket)
	key := aws.String(testKey)
	etag := aws.String(testETag)
	rangeHeader := aws.String(testRangeHeader)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		bodyMock := closerMock.NewMockreaderCloser(t)

		clientMock := mocks.NewMocks3Getter(t)
		clientMock.On("GetObject", ctx, &s3.GetObjectInput{
			Bucket:  bucket,
			Key:     key,
			Range:   rangeHeader,
			IfMatch: etag,
		}).Return(&s3.GetObjectOutput{
			Body: bodyMock,
		}, nil)

		reader := &rangeReader{
			client: clientMock,
			bucket: bucket,
			key:    key,
			etag:   etag,
			size:   testSize,
		}

		body, err := reader.OpenRange(ctx, rangeHeader)

		require.NoError(t, err)
		require.NotNil(t, body)
		require.Equal(t, bodyMock, body)
	})

	t.Run("Success with nil range header", func(t *testing.T) {
		t.Parallel()

		bodyMock := closerMock.NewMockreaderCloser(t)

		clientMock := mocks.NewMocks3Getter(t)
		clientMock.On("GetObject", ctx, &s3.GetObjectInput{
			Bucket:  bucket,
			Key:     key,
			Range:   nil,
			IfMatch: etag,
		}).Return(&s3.GetObjectOutput{
			Body: bodyMock,
		}, nil)

		reader := &rangeReader{
			client: clientMock,
			bucket: bucket,
			key:    key,
			etag:   etag,
			size:   testSize,
		}

		body, err := reader.OpenRange(ctx, nil)

		require.NoError(t, err)
		require.NotNil(t, body)
	})

	t.Run("Success with different range", func(t *testing.T) {
		t.Parallel()

		differentRange := aws.String("bytes=1024-2047")
		bodyMock := closerMock.NewMockreaderCloser(t)

		clientMock := mocks.NewMocks3Getter(t)
		clientMock.On("GetObject", ctx, &s3.GetObjectInput{
			Bucket:  bucket,
			Key:     key,
			Range:   differentRange,
			IfMatch: etag,
		}).Return(&s3.GetObjectOutput{
			Body: bodyMock,
		}, nil)

		reader := &rangeReader{
			client: clientMock,
			bucket: bucket,
			key:    key,
			etag:   etag,
			size:   testSize,
		}

		body, err := reader.OpenRange(ctx, differentRange)

		require.NoError(t, err)
		require.NotNil(t, body)
	})

	t.Run("Error GetObject failed", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMocks3Getter(t)
		clientMock.On("GetObject", ctx, &s3.GetObjectInput{
			Bucket:  bucket,
			Key:     key,
			Range:   rangeHeader,
			IfMatch: etag,
		}).Return(nil, errS3Test)

		reader := &rangeReader{
			client: clientMock,
			bucket: bucket,
			key:    key,
			etag:   etag,
			size:   testSize,
		}

		body, err := reader.OpenRange(ctx, rangeHeader)

		require.ErrorIs(t, err, errS3Test)
		require.Contains(t, err.Error(), "failed to get object")
		require.Contains(t, err.Error(), testKey)
		require.Nil(t, body)
	})
}

func TestRangeReader_GetSize(t *testing.T) {
	t.Parallel()

	t.Run("Returns correct size", func(t *testing.T) {
		t.Parallel()

		reader := &rangeReader{
			size: testSize,
		}

		size := reader.GetSize()

		require.Equal(t, testSize, size)
	})

	t.Run("Returns zero size", func(t *testing.T) {
		t.Parallel()

		reader := &rangeReader{
			size: 0,
		}

		size := reader.GetSize()

		require.Equal(t, int64(0), size)
	})

	t.Run("Returns large size", func(t *testing.T) {
		t.Parallel()

		largeSize := int64(9223372036854775807) // max int64

		reader := &rangeReader{
			size: largeSize,
		}

		size := reader.GetSize()

		require.Equal(t, largeSize, size)
	})
}

func TestRangeReader_GetInfo(t *testing.T) {
	t.Parallel()

	t.Run("Returns correct info", func(t *testing.T) {
		t.Parallel()

		bucket := aws.String(testBucket)
		key := aws.String(testKey)

		reader := &rangeReader{
			bucket: bucket,
			key:    key,
		}

		info := reader.GetInfo()

		require.Equal(t, fmt.Sprintf("%s:%s", testBucket, testKey), info)
	})

	t.Run("Returns info with path", func(t *testing.T) {
		t.Parallel()

		bucket := aws.String("my-backup-bucket")
		key := aws.String("backups/2024/file.txt")

		reader := &rangeReader{
			bucket: bucket,
			key:    key,
		}

		info := reader.GetInfo()

		require.Equal(t, "my-backup-bucket:backups/2024/file.txt", info)
	})

	t.Run("Returns info with empty values", func(t *testing.T) {
		t.Parallel()

		bucket := aws.String("")
		key := aws.String("")

		reader := &rangeReader{
			bucket: bucket,
			key:    key,
		}

		info := reader.GetInfo()

		require.Equal(t, ":", info)
	})
}
