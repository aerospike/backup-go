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

package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"cloud.google.com/go/storage"
	closerMock "github.com/aerospike/backup-go/io/storage/common/mocks"
	"github.com/aerospike/backup-go/io/storage/gcp/storage/mocks"
	"github.com/stretchr/testify/require"
)

const (
	testBucket     = "test-bucket"
	testPath       = "test-path"
	testGeneration = int64(1234567890)
	testSize       = int64(12345)
)

var (
	errGCPTest = errors.New("gcp test error")
)

func TestNewRangeReader(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMockgcpGetter(t)
		clientMock.On("GetAttrs", ctx, testPath).
			Return(&storage.ObjectAttrs{
				Generation: testGeneration,
				Size:       testSize,
			}, nil)

		reader, err := newRangeReader(ctx, clientMock, testBucket, testPath)

		require.NoError(t, err)
		require.NotNil(t, reader)
		require.Equal(t, testSize, reader.size)
		require.Equal(t, testGeneration, reader.generation)
		require.Equal(t, testBucket, reader.bucket)
		require.Equal(t, testPath, reader.path)
		require.Equal(t, clientMock, reader.client)
	})

	t.Run("Success with zero size", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMockgcpGetter(t)
		clientMock.On("GetAttrs", ctx, testPath).
			Return(&storage.ObjectAttrs{
				Generation: testGeneration,
				Size:       0,
			}, nil)

		reader, err := newRangeReader(ctx, clientMock, testBucket, testPath)

		require.NoError(t, err)
		require.NotNil(t, reader)
		require.Equal(t, int64(0), reader.size)
		require.Equal(t, testGeneration, reader.generation)
	})

	t.Run("Success with large size", func(t *testing.T) {
		t.Parallel()

		largeSize := int64(9223372036854775807) // max int64

		clientMock := mocks.NewMockgcpGetter(t)
		clientMock.On("GetAttrs", ctx, testPath).
			Return(&storage.ObjectAttrs{
				Generation: testGeneration,
				Size:       largeSize,
			}, nil)

		reader, err := newRangeReader(ctx, clientMock, testBucket, testPath)

		require.NoError(t, err)
		require.NotNil(t, reader)
		require.Equal(t, largeSize, reader.size)
	})

	t.Run("Success with zero generation", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMockgcpGetter(t)
		clientMock.On("GetAttrs", ctx, testPath).
			Return(&storage.ObjectAttrs{
				Generation: 0,
				Size:       testSize,
			}, nil)

		reader, err := newRangeReader(ctx, clientMock, testBucket, testPath)

		require.NoError(t, err)
		require.NotNil(t, reader)
		require.Equal(t, int64(0), reader.generation)
	})

	t.Run("Error GetAttrs failed", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMockgcpGetter(t)
		clientMock.On("GetAttrs", ctx, testPath).
			Return(nil, errGCPTest)

		reader, err := newRangeReader(ctx, clientMock, testBucket, testPath)

		require.ErrorIs(t, err, errGCPTest)
		require.Contains(t, err.Error(), "failed to get attr")
		require.Contains(t, err.Error(), testPath)
		require.Nil(t, reader)
	})
}

func TestRangeReader_OpenRange(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("Success with offset and count", func(t *testing.T) {
		t.Parallel()

		offset := int64(100)
		count := int64(1024)

		bodyMock := closerMock.NewMockreaderCloser(t)

		clientMock := mocks.NewMockgcpGetter(t)
		clientMock.On("GetReader", ctx, testPath, testGeneration, offset, count).
			Return(bodyMock, nil)

		reader := &rangeReader{
			client:     clientMock,
			bucket:     testBucket,
			path:       testPath,
			generation: testGeneration,
			size:       testSize,
		}

		body, err := reader.OpenRange(ctx, offset, count)

		require.NoError(t, err)
		require.NotNil(t, body)
		require.Equal(t, bodyMock, body)
	})

	t.Run("Success with zero offset and count", func(t *testing.T) {
		t.Parallel()

		bodyMock := closerMock.NewMockreaderCloser(t)

		clientMock := mocks.NewMockgcpGetter(t)
		clientMock.On("GetReader", ctx, testPath, testGeneration, int64(0), int64(0)).
			Return(bodyMock, nil)

		reader := &rangeReader{
			client:     clientMock,
			bucket:     testBucket,
			path:       testPath,
			generation: testGeneration,
			size:       testSize,
		}

		body, err := reader.OpenRange(ctx, 0, 0)

		require.NoError(t, err)
		require.NotNil(t, body)
		require.Equal(t, bodyMock, body)
	})

	t.Run("Success with offset and zero count (read to end)", func(t *testing.T) {
		t.Parallel()

		offset := int64(100)
		bodyMock := closerMock.NewMockreaderCloser(t)

		clientMock := mocks.NewMockgcpGetter(t)
		clientMock.On("GetReader", ctx, testPath, testGeneration, offset, int64(0)).
			Return(bodyMock, nil)

		reader := &rangeReader{
			client:     clientMock,
			bucket:     testBucket,
			path:       testPath,
			generation: testGeneration,
			size:       testSize,
		}

		body, err := reader.OpenRange(ctx, offset, 0)

		require.NoError(t, err)
		require.NotNil(t, body)
		require.Equal(t, bodyMock, body)
	})

	t.Run("Success with large offset and count", func(t *testing.T) {
		t.Parallel()

		offset := int64(1048576) // 1MB
		count := int64(10485760) // 10MB
		bodyMock := closerMock.NewMockreaderCloser(t)

		clientMock := mocks.NewMockgcpGetter(t)
		clientMock.On("GetReader", ctx, testPath, testGeneration, offset, count).
			Return(bodyMock, nil)

		reader := &rangeReader{
			client:     clientMock,
			bucket:     testBucket,
			path:       testPath,
			generation: testGeneration,
			size:       testSize,
		}

		body, err := reader.OpenRange(ctx, offset, count)

		require.NoError(t, err)
		require.NotNil(t, body)
		require.Equal(t, bodyMock, body)
	})

	t.Run("Success with negative count (read to end)", func(t *testing.T) {
		t.Parallel()

		offset := int64(100)
		count := int64(-1) // GCP convention for "read to end"
		bodyMock := closerMock.NewMockreaderCloser(t)

		clientMock := mocks.NewMockgcpGetter(t)
		clientMock.On("GetReader", ctx, testPath, testGeneration, offset, count).
			Return(bodyMock, nil)

		reader := &rangeReader{
			client:     clientMock,
			bucket:     testBucket,
			path:       testPath,
			generation: testGeneration,
			size:       testSize,
		}

		body, err := reader.OpenRange(ctx, offset, count)

		require.NoError(t, err)
		require.NotNil(t, body)
		require.Equal(t, bodyMock, body)
	})

	t.Run("Error GetReader failed", func(t *testing.T) {
		t.Parallel()

		offset := int64(100)
		count := int64(1024)

		clientMock := mocks.NewMockgcpGetter(t)
		clientMock.On("GetReader", ctx, testPath, testGeneration, offset, count).
			Return(nil, errGCPTest)

		reader := &rangeReader{
			client:     clientMock,
			bucket:     testBucket,
			path:       testPath,
			generation: testGeneration,
			size:       testSize,
		}

		body, err := reader.OpenRange(ctx, offset, count)

		require.ErrorIs(t, err, errGCPTest)
		require.Contains(t, err.Error(), "failed to get reader")
		require.Contains(t, err.Error(), testPath)
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

		reader := &rangeReader{
			bucket: testBucket,
			path:   testPath,
		}

		info := reader.GetInfo()

		require.Equal(t, fmt.Sprintf("%s:%s", testBucket, testPath), info)
	})

	t.Run("Returns info with nested path", func(t *testing.T) {
		t.Parallel()

		reader := &rangeReader{
			bucket: "my-backup-bucket",
			path:   "backups/2024/10/file.bin",
		}

		info := reader.GetInfo()

		require.Equal(t, "my-backup-bucket:backups/2024/10/file.bin", info)
	})

	t.Run("Returns info with empty values", func(t *testing.T) {
		t.Parallel()

		reader := &rangeReader{
			bucket: "",
			path:   "",
		}

		info := reader.GetInfo()

		require.Equal(t, ":", info)
	})
}
