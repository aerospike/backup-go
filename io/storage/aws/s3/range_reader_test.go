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
	"errors"
	"fmt"
	"testing"

	"github.com/aerospike/backup-go/io/storage/aws/s3/mocks"
	closerMock "github.com/aerospike/backup-go/io/storage/common/mocks"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

const (
	testKey    = "test-key"
	testETag   = "test-etag"
	testSize   = int64(12345)
	testOffset = int64(100)
	testCount  = 0
)

var (
	errS3Test = errors.New("test error")
)

func newTestTransferManager(client Client) *transfermanager.Client {
	return transfermanager.New(client, func(o *transfermanager.Options) {
		o.GetObjectType = tmtypes.GetObjectRanges
		o.PartSizeBytes = s3DefaultChunkSize
		o.Concurrency = 1
		o.DisableChecksumValidation = true
	})
}

func TestNewDownloadReader(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	bucket := aws.String(testBucket)
	key := aws.String(testKey)
	etag := aws.String(testETag)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMockClient(t)
		clientMock.On("HeadObject", ctx, &s3.HeadObjectInput{
			Bucket: bucket,
			Key:    key,
		}).Return(&s3.HeadObjectOutput{
			ContentLength: aws.Int64(testSize),
			ETag:          etag,
		}, nil)

		tm := newTestTransferManager(clientMock)
		reader, err := newDownloadReader(ctx, clientMock, tm, bucket, key, 0)

		require.NoError(t, err)
		require.NotNil(t, reader)
		require.Equal(t, testSize, reader.size)
		require.Equal(t, bucket, reader.bucket)
		require.Equal(t, key, reader.key)
		require.Equal(t, etag, reader.etag)
		require.Equal(t, clientMock, reader.client)
		require.NotNil(t, reader.tm)
	})

	t.Run("Success with nil ContentLength", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMockClient(t)
		clientMock.On("HeadObject", ctx, &s3.HeadObjectInput{
			Bucket: bucket,
			Key:    key,
		}).Return(&s3.HeadObjectOutput{
			ContentLength: nil,
			ETag:          etag,
		}, nil)

		tm := newTestTransferManager(clientMock)
		reader, err := newDownloadReader(ctx, clientMock, tm, bucket, key, 0)

		require.NoError(t, err)
		require.NotNil(t, reader)
		require.Equal(t, int64(0), reader.size)
		require.Equal(t, etag, reader.etag)
	})

	t.Run("Success with zero ContentLength", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMockClient(t)
		clientMock.On("HeadObject", ctx, &s3.HeadObjectInput{
			Bucket: bucket,
			Key:    key,
		}).Return(&s3.HeadObjectOutput{
			ContentLength: aws.Int64(0),
			ETag:          etag,
		}, nil)

		tm := newTestTransferManager(clientMock)
		reader, err := newDownloadReader(ctx, clientMock, tm, bucket, key, 0)

		require.NoError(t, err)
		require.NotNil(t, reader)
		require.Equal(t, int64(0), reader.size)
	})

	t.Run("Success with nil ETag", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMockClient(t)
		clientMock.On("HeadObject", ctx, &s3.HeadObjectInput{
			Bucket: bucket,
			Key:    key,
		}).Return(&s3.HeadObjectOutput{
			ContentLength: aws.Int64(testSize),
			ETag:          nil,
		}, nil)

		tm := newTestTransferManager(clientMock)
		reader, err := newDownloadReader(ctx, clientMock, tm, bucket, key, 0)

		require.NoError(t, err)
		require.NotNil(t, reader)
		require.Nil(t, reader.etag)
	})

	t.Run("Error HeadObject failed", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMockClient(t)
		clientMock.On("HeadObject", ctx, &s3.HeadObjectInput{
			Bucket: bucket,
			Key:    key,
		}).Return(nil, errS3Test)

		tm := newTestTransferManager(clientMock)
		reader, err := newDownloadReader(ctx, clientMock, tm, bucket, key, 0)

		require.ErrorIs(t, err, errS3Test)
		require.Contains(t, err.Error(), "failed to get head")
		require.Contains(t, err.Error(), testKey)
		require.Nil(t, reader)
	})
}

func TestDownloadReader_OpenRange(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	bucket := aws.String(testBucket)
	key := aws.String(testKey)
	etag := aws.String(testETag)
	rangeHeader := aws.String(fmt.Sprintf("bytes=%d-", testOffset))
	t.Run("Success offset resume uses raw GetObject", func(t *testing.T) {
		t.Parallel()

		bodyMock := closerMock.NewMockreaderCloser(t)

		clientMock := mocks.NewMockClient(t)
		clientMock.On("GetObject", ctx, &s3.GetObjectInput{
			Bucket:  bucket,
			Key:     key,
			Range:   rangeHeader,
			IfMatch: etag,
		}).Return(&s3.GetObjectOutput{
			Body: bodyMock,
		}, nil)

		reader := &downloadReader{
			client: clientMock,
			tm:     newTestTransferManager(clientMock),
			bucket: bucket,
			key:    key,
			etag:   etag,
			size:   testSize,
		}

		body, err := reader.OpenRange(ctx, testOffset, testCount)

		require.NoError(t, err)
		require.NotNil(t, body)
		require.Equal(t, bodyMock, body)
	})

	t.Run("Success with different range", func(t *testing.T) {
		t.Parallel()

		differentRange := aws.String("bytes=1024-2047")
		bodyMock := closerMock.NewMockreaderCloser(t)

		clientMock := mocks.NewMockClient(t)
		clientMock.On("GetObject", ctx, &s3.GetObjectInput{
			Bucket:  bucket,
			Key:     key,
			Range:   differentRange,
			IfMatch: etag,
		}).Return(&s3.GetObjectOutput{
			Body: bodyMock,
		}, nil)

		reader := &downloadReader{
			client: clientMock,
			tm:     newTestTransferManager(clientMock),
			bucket: bucket,
			key:    key,
			etag:   etag,
			size:   testSize,
		}

		body, err := reader.OpenRange(ctx, 1024, 2047)

		require.NoError(t, err)
		require.NotNil(t, body)
	})

	t.Run("Error GetObject failed", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMockClient(t)
		clientMock.On("GetObject", ctx, &s3.GetObjectInput{
			Bucket:  bucket,
			Key:     key,
			Range:   rangeHeader,
			IfMatch: etag,
		}).Return(nil, errS3Test)

		reader := &downloadReader{
			client: clientMock,
			tm:     newTestTransferManager(clientMock),
			bucket: bucket,
			key:    key,
			etag:   etag,
			size:   testSize,
		}

		body, err := reader.OpenRange(ctx, testOffset, testCount)

		require.ErrorIs(t, err, errS3Test)
		require.Contains(t, err.Error(), "failed to get object")
		require.Contains(t, err.Error(), testKey)
		require.Nil(t, body)
	})
}

func TestDownloadReader_GetSize(t *testing.T) {
	t.Parallel()

	t.Run("Returns correct size", func(t *testing.T) {
		t.Parallel()

		reader := &downloadReader{
			size: testSize,
		}

		size := reader.GetSize()

		require.Equal(t, testSize, size)
	})

	t.Run("Returns zero size", func(t *testing.T) {
		t.Parallel()

		reader := &downloadReader{
			size: 0,
		}

		size := reader.GetSize()

		require.Equal(t, int64(0), size)
	})

	t.Run("Returns large size", func(t *testing.T) {
		t.Parallel()

		largeSize := int64(9223372036854775807) // max int64

		reader := &downloadReader{
			size: largeSize,
		}

		size := reader.GetSize()

		require.Equal(t, largeSize, size)
	})
}

func TestDownloadReader_GetInfo(t *testing.T) {
	t.Parallel()

	t.Run("Returns correct info", func(t *testing.T) {
		t.Parallel()

		bucket := aws.String(testBucket)
		key := aws.String(testKey)

		reader := &downloadReader{
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

		reader := &downloadReader{
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

		reader := &downloadReader{
			bucket: bucket,
			key:    key,
		}

		info := reader.GetInfo()

		require.Equal(t, ":", info)
	})
}
