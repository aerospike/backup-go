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

package blob

import (
	"context"
	"errors"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/aerospike/backup-go/io/storage/azure/blob/mocks"
	closerMock "github.com/aerospike/backup-go/io/storage/common/mocks"
	"github.com/stretchr/testify/require"
)

const (
	testContainer = "test-container"
	testPath      = "test-path"
	testSize      = int64(12345)
)

// testETag we need pointer to this value, so it will be variable.
var testETag = azcore.ETag("test-etag")

var (
	errBlobTest = errors.New("blob test error")
)

func int64Ptr(i int64) *int64 {
	return &i
}

func defaultReader() *rangeReader {
	return &rangeReader{
		container: testContainer,
		path:      testPath,
	}
}

func TestNewRangeReader(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMockazblobGetter(t)
		clientMock.On("GetBlobProperties", ctx, testContainer, testPath).
			Return(blob.GetPropertiesResponse{
				ContentLength: int64Ptr(testSize),
			}, nil)

		reader, err := newRangeReader(ctx, clientMock, testContainer, testPath)

		require.NoError(t, err)
		require.NotNil(t, reader)
		require.Equal(t, testSize, reader.size)
		require.Equal(t, testContainer, reader.container)
		require.Equal(t, testPath, reader.path)
		require.Equal(t, clientMock, reader.client)
	})

	t.Run("Success with nil ContentLength", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMockazblobGetter(t)
		clientMock.On("GetBlobProperties", ctx, testContainer, testPath).
			Return(blob.GetPropertiesResponse{
				ContentLength: nil,
			}, nil)

		reader, err := newRangeReader(ctx, clientMock, testContainer, testPath)

		require.NoError(t, err)
		require.NotNil(t, reader)
		require.Equal(t, int64(0), reader.size)
	})

	t.Run("Success with zero ContentLength", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMockazblobGetter(t)
		clientMock.On("GetBlobProperties", ctx, testContainer, testPath).
			Return(blob.GetPropertiesResponse{
				ContentLength: int64Ptr(0),
			}, nil)

		reader, err := newRangeReader(ctx, clientMock, testContainer, testPath)

		require.NoError(t, err)
		require.NotNil(t, reader)
		require.Equal(t, int64(0), reader.size)
	})

	t.Run("Error GetBlobProperties failed", func(t *testing.T) {
		t.Parallel()

		clientMock := mocks.NewMockazblobGetter(t)
		clientMock.On("GetBlobProperties", ctx, testContainer, testPath).
			Return(blob.GetPropertiesResponse{}, errBlobTest)

		reader, err := newRangeReader(ctx, clientMock, testContainer, testPath)

		require.ErrorIs(t, err, errBlobTest)
		require.Contains(t, err.Error(), "failed to get properties")
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

		clientMock := mocks.NewMockazblobGetter(t)
		clientMock.On("DownloadStream", ctx, testContainer, testPath, &blob.DownloadStreamOptions{
			Range: blob.HTTPRange{
				Offset: offset,
				Count:  count,
			},
			AccessConditions: &blob.AccessConditions{
				ModifiedAccessConditions: &blob.ModifiedAccessConditions{
					IfMatch: &testETag,
				},
			},
		}).Return(azblob.DownloadStreamResponse{
			DownloadResponse: blob.DownloadResponse{
				Body: bodyMock,
			},
		}, nil)

		reader := &rangeReader{
			client:    clientMock,
			container: testContainer,
			path:      testPath,
			size:      testSize,
			etag:      &testETag,
		}

		body, err := reader.OpenRange(ctx, offset, count)

		require.NoError(t, err)
		require.NotNil(t, body)
		require.Equal(t, bodyMock, body)
	})

	t.Run("Success with zero offset and count", func(t *testing.T) {
		t.Parallel()

		bodyMock := closerMock.NewMockreaderCloser(t)

		clientMock := mocks.NewMockazblobGetter(t)
		clientMock.On("DownloadStream", ctx, testContainer, testPath, (*blob.DownloadStreamOptions)(nil)).
			Return(azblob.DownloadStreamResponse{
				DownloadResponse: blob.DownloadResponse{
					Body: bodyMock,
				},
			}, nil)

		reader := &rangeReader{
			client:    clientMock,
			container: testContainer,
			path:      testPath,
			size:      testSize,
		}

		body, err := reader.OpenRange(ctx, 0, 0)

		require.NoError(t, err)
		require.NotNil(t, body)
	})

	t.Run("Success with offset and zero count", func(t *testing.T) {
		t.Parallel()

		offset := int64(100)

		bodyMock := closerMock.NewMockreaderCloser(t)

		clientMock := mocks.NewMockazblobGetter(t)
		clientMock.On("DownloadStream", ctx, testContainer, testPath, &blob.DownloadStreamOptions{
			Range: blob.HTTPRange{
				Offset: offset,
				Count:  0,
			},
			AccessConditions: &blob.AccessConditions{
				ModifiedAccessConditions: &blob.ModifiedAccessConditions{
					IfMatch: &testETag,
				},
			},
		}).Return(azblob.DownloadStreamResponse{
			DownloadResponse: blob.DownloadResponse{
				Body: bodyMock,
			},
		}, nil)

		reader := &rangeReader{
			client:    clientMock,
			container: testContainer,
			path:      testPath,
			size:      testSize,
			etag:      &testETag,
		}

		body, err := reader.OpenRange(ctx, offset, 0)

		require.NoError(t, err)
		require.NotNil(t, body)
	})

	t.Run("Success with large offset and count", func(t *testing.T) {
		t.Parallel()

		offset := int64(1048576) // 1MB
		count := int64(10485760) // 10MB

		bodyMock := closerMock.NewMockreaderCloser(t)

		clientMock := mocks.NewMockazblobGetter(t)
		clientMock.On("DownloadStream", ctx, testContainer, testPath, &blob.DownloadStreamOptions{
			Range: blob.HTTPRange{
				Offset: offset,
				Count:  count,
			},
			AccessConditions: &blob.AccessConditions{
				ModifiedAccessConditions: &blob.ModifiedAccessConditions{
					IfMatch: &testETag,
				},
			},
		}).Return(azblob.DownloadStreamResponse{
			DownloadResponse: blob.DownloadResponse{
				Body: bodyMock,
			},
		}, nil)

		reader := &rangeReader{
			client:    clientMock,
			container: testContainer,
			path:      testPath,
			size:      testSize,
			etag:      &testETag,
		}

		body, err := reader.OpenRange(ctx, offset, count)

		require.NoError(t, err)
		require.NotNil(t, body)
	})

	t.Run("Error DownloadStream failed", func(t *testing.T) {
		t.Parallel()

		offset := int64(100)
		count := int64(1024)

		clientMock := mocks.NewMockazblobGetter(t)
		clientMock.On("DownloadStream", ctx, testContainer, testPath, &blob.DownloadStreamOptions{
			Range: blob.HTTPRange{
				Offset: offset,
				Count:  count,
			},
			AccessConditions: &blob.AccessConditions{
				ModifiedAccessConditions: &blob.ModifiedAccessConditions{
					IfMatch: &testETag,
				},
			},
		}).Return(azblob.DownloadStreamResponse{}, errBlobTest)

		reader := &rangeReader{
			client:    clientMock,
			container: testContainer,
			path:      testPath,
			size:      testSize,
			etag:      &testETag,
		}

		body, err := reader.OpenRange(ctx, offset, count)

		require.ErrorIs(t, err, errBlobTest)
		require.Contains(t, err.Error(), "failed to download stream")
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
			container: testContainer,
			path:      testPath,
		}

		info := reader.GetInfo()

		require.Equal(t, "test-container:test-path", info)
	})

	t.Run("Returns info with nested path", func(t *testing.T) {
		t.Parallel()

		reader := &rangeReader{
			container: "my-backup-container",
			path:      "backups/2024/10/file.bin",
		}

		info := reader.GetInfo()

		require.Equal(t, "my-backup-container:backups/2024/10/file.bin", info)
	})

	t.Run("Returns info with empty values", func(t *testing.T) {
		t.Parallel()

		reader := &rangeReader{
			container: "",
			path:      "",
		}

		info := reader.GetInfo()

		require.Equal(t, ":", info)
	})
}

func TestGetStreamOptions(t *testing.T) {
	t.Parallel()

	t.Run("Returns nil for zero offset and count", func(t *testing.T) {
		t.Parallel()

		r := defaultReader()

		opts := r.getStreamOptions(0, 0)

		require.Nil(t, opts)
	})

	t.Run("Returns options with offset and count", func(t *testing.T) {
		t.Parallel()

		offset := int64(100)
		count := int64(1024)

		r := defaultReader()

		opts := r.getStreamOptions(offset, count)

		require.NotNil(t, opts)
		require.Equal(t, offset, opts.Range.Offset)
		require.Equal(t, count, opts.Range.Count)
	})

	t.Run("Returns options with offset and zero count", func(t *testing.T) {
		t.Parallel()

		offset := int64(100)

		r := defaultReader()

		opts := r.getStreamOptions(offset, 0)

		require.NotNil(t, opts)
		require.Equal(t, offset, opts.Range.Offset)
		require.Equal(t, int64(0), opts.Range.Count)
	})

	t.Run("Returns options with zero offset and count", func(t *testing.T) {
		t.Parallel()

		count := int64(1024)

		r := defaultReader()

		opts := r.getStreamOptions(0, count)

		require.NotNil(t, opts)
		require.Equal(t, int64(0), opts.Range.Offset)
		require.Equal(t, count, opts.Range.Count)
	})

	t.Run("Returns options with large values", func(t *testing.T) {
		t.Parallel()

		offset := int64(1048576)  // 1MB
		count := int64(104857600) // 100MB

		r := defaultReader()

		opts := r.getStreamOptions(offset, count)

		require.NotNil(t, opts)
		require.Equal(t, offset, opts.Range.Offset)
		require.Equal(t, count, opts.Range.Count)
	})
}
