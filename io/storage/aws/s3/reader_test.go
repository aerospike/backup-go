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
	"testing"
	"time"

	"github.com/aerospike/backup-go/io/storage/aws/s3/mocks"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const testBucket = "asbackup"

func TestParseStorageClass(t *testing.T) {
	tests := []struct {
		name          string
		class         string
		expected      types.StorageClass
		expectedError error
	}{
		{
			name:          "standard storage class",
			class:         "STANDARD",
			expected:      types.StorageClassStandard,
			expectedError: nil,
		},
		{
			name:          "reduced redundancy storage class",
			class:         "REDUCED_REDUNDANCY",
			expected:      types.StorageClassReducedRedundancy,
			expectedError: nil,
		},
		{
			name:          "glacier storage class",
			class:         "GLACIER",
			expected:      types.StorageClassGlacier,
			expectedError: nil,
		},
		{
			name:          "standard ia storage class",
			class:         "STANDARD_IA",
			expected:      types.StorageClassStandardIa,
			expectedError: nil,
		},
		{
			name:          "onezone ia storage class",
			class:         "ONEZONE_IA",
			expected:      types.StorageClassOnezoneIa,
			expectedError: nil,
		},
		{
			name:          "intelligent tiering storage class",
			class:         "INTELLIGENT_TIERING",
			expected:      types.StorageClassIntelligentTiering,
			expectedError: nil,
		},
		{
			name:          "deep archive storage class",
			class:         "DEEP_ARCHIVE",
			expected:      types.StorageClassDeepArchive,
			expectedError: nil,
		},
		{
			name:          "outposts storage class",
			class:         "OUTPOSTS",
			expected:      types.StorageClassOutposts,
			expectedError: nil,
		},
		{
			name:          "glacier ir storage class",
			class:         "GLACIER_IR",
			expected:      types.StorageClassGlacierIr,
			expectedError: nil,
		},
		{
			name:          "lower case input",
			class:         "standard",
			expected:      types.StorageClassStandard,
			expectedError: nil,
		},
		{
			name:          "mixed case input",
			class:         "StAnDaRd",
			expected:      types.StorageClassStandard,
			expectedError: nil,
		},
		{
			name:          "empty input",
			class:         "",
			expected:      "",
			expectedError: fmt.Errorf("invalid storage class "),
		},
		{
			name:          "invalid storage class",
			class:         "INVALID_STORAGE_CLASS",
			expected:      "",
			expectedError: fmt.Errorf("invalid storage class INVALID_STORAGE_CLASS"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := parseStorageClass(tc.class)

			if tc.expectedError != nil {
				require.Error(t, err)
				assert.Equal(t, tc.expectedError.Error(), err.Error())
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestParseAccessTier(t *testing.T) {
	tests := []struct {
		name          string
		tier          string
		expected      types.Tier
		expectedError error
	}{
		{
			name:          "expedited tier",
			tier:          "Expedited",
			expected:      types.TierExpedited,
			expectedError: nil,
		},
		{
			name:          "standard tier",
			tier:          "Standard",
			expected:      types.TierStandard,
			expectedError: nil,
		},
		{
			name:          "bulk tier",
			tier:          "Bulk",
			expected:      types.TierBulk,
			expectedError: nil,
		},
		{
			name:          "lower case input",
			tier:          "expedited",
			expected:      types.TierExpedited,
			expectedError: nil,
		},
		{
			name:          "mixed case input",
			tier:          "sTaNdArD",
			expected:      types.TierStandard,
			expectedError: nil,
		},
		{
			name:          "empty input",
			tier:          "",
			expected:      "",
			expectedError: fmt.Errorf("invalid access tier "),
		},
		{
			name:          "invalid tier",
			tier:          "INVALID_TIER",
			expected:      "",
			expectedError: fmt.Errorf("invalid access tier Invalid_tier"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := parseAccessTier(tc.tier)

			if tc.expectedError != nil {
				require.Error(t, err)
				assert.Equal(t, tc.expectedError.Error(), err.Error())
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestReader_ShouldSkip(t *testing.T) {
	testSize := int64(1000)
	testEmptySize := int64(0)

	tests := []struct {
		name          string
		path          string
		fileName      *string
		fileSize      *int64
		withNestedDir bool
		expected      bool
	}{
		{
			name:          "nil filename",
			path:          "test/path",
			fileName:      nil,
			fileSize:      &testSize,
			withNestedDir: false,
			expected:      true,
		},
		{
			name:          "directory with nested dir enabled",
			path:          "test/path/",
			fileName:      aws.String("test/path/subdir/"),
			fileSize:      &testSize,
			withNestedDir: true,
			expected:      false,
		},
		{
			name:          "directory with nested dir disabled",
			path:          "test/path/",
			fileName:      aws.String("test/path/subdir/"),
			fileSize:      &testSize,
			withNestedDir: false,
			expected:      true,
		},
		{
			name:          "regular file",
			path:          "test/path/",
			fileName:      aws.String("test/path/file.txt"),
			fileSize:      &testSize,
			withNestedDir: false,
			expected:      false,
		},
		{
			name:          "nil filename",
			path:          "test/path",
			fileName:      aws.String("file.txt"),
			fileSize:      &testEmptySize,
			withNestedDir: false,
			expected:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := &Reader{
				Options: options.Options{
					WithNestedDir: tc.withNestedDir,
				},
			}
			result := reader.shouldSkip(tc.path, tc.fileName, tc.fileSize)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestNewReader_WithAccessTier_Standard tests warming with Standard tier.
func TestNewReader_WithAccessTier_Standard(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt"), Size: aws.Int64(100)},
			},
		}, nil).
		Once()

	// For warmDirectory - ListObjects.
	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt"), Size: aws.Int64(100)},
			},
		}, nil).
		Once()

	// For checkObjectAvailability - object already available
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(&s3.HeadObjectOutput{
			StorageClass: types.StorageClassStandard,
		}, nil).
		Once()

	opts := func(o *options.Options) {
		o.PathList = []string{testDir}
		o.IsDir = true
		o.AccessTier = "Standard"
		o.PollWarmDuration = 10 * time.Millisecond
	}

	reader, err := NewReader(ctx, mockClient, testBucket, opts)

	require.NoError(t, err)
	assert.NotNil(t, reader)
}

// TestNewReader_WithAccessTier_Expedited tests warming with Expedited tier.
func TestNewReader_WithAccessTier_Expedited(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt"), Size: aws.Int64(100)},
			},
		}, nil).
		Once()

	// For warmDirectory.
	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt"), Size: aws.Int64(100)},
			},
		}, nil).
		Once()

	// For checkObjectAvailability - archived.
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(&s3.HeadObjectOutput{
			StorageClass: types.StorageClassGlacier,
		}, nil).
		Once()

	// For restoreObject.
	mockClient.EXPECT().
		RestoreObject(ctx, mock.Anything).
		Return(&s3.RestoreObjectOutput{}, nil).
		Once()

	// For pollWarmDirStatus - now available.
	restoreStatus := restoreValueFinished
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(&s3.HeadObjectOutput{
			Restore: &restoreStatus,
		}, nil).
		Once()

	opts := func(o *options.Options) {
		o.PathList = []string{testDir}
		o.IsDir = true
		o.AccessTier = "Expedited"
		o.PollWarmDuration = 10 * time.Millisecond
	}

	reader, err := NewReader(ctx, mockClient, testBucket, opts)

	time.Sleep(15 * time.Millisecond)

	require.NoError(t, err)
	assert.NotNil(t, reader)
}

// TestReader_RestoreObject_Success tests successful object restoration.
func TestReader_RestoreObject_Success(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	reader, err := NewReader(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)
	require.NoError(t, err)

	mockClient.EXPECT().
		RestoreObject(ctx, mock.MatchedBy(func(input *s3.RestoreObjectInput) bool {
			return *input.Bucket == testBucket &&
				*input.Key == testFile &&
				input.RestoreRequest.GlacierJobParameters.Tier == types.TierStandard
		})).
		Return(&s3.RestoreObjectOutput{}, nil).
		Once()

	err = reader.restoreObject(ctx, testFile, types.TierStandard)

	require.NoError(t, err)
}

// TestReader_RestoreObject_Error tests error handling in restoreObject.
func TestReader_RestoreObject_Error(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	reader, err := NewReader(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)
	require.NoError(t, err)

	mockClient.EXPECT().
		RestoreObject(ctx, mock.Anything).
		Return(nil, errors.New("restore failed")).
		Once()

	err = reader.restoreObject(ctx, testFile, types.TierStandard)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to restore object")
}

// TestReader_WarmStorage_Success tests successful storage warming.
func TestReader_WarmStorage_Success(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	reader, err := NewReader(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)
	require.NoError(t, err)
	reader.PathList = []string{testDir}
	reader.objectsToWarm = make([]string, 0)

	// For ListObjects in warmDirectory.
	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt"), Size: aws.Int64(100)},
			},
		}, nil).
		Once()

	// For checkObjectAvailability - available.
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(&s3.HeadObjectOutput{
			StorageClass: types.StorageClassStandard,
		}, nil).
		Once()

	err = reader.warmStorage(ctx, types.TierStandard)

	require.NoError(t, err)
}

// TestReader_WarmDirectory_ArchivedObject tests warming archived object.
func TestReader_WarmDirectory_ArchivedObject(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	reader, err := NewReader(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)
	require.NoError(t, err)
	reader.objectsToWarm = make([]string, 0)

	// For ListObjects.
	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt"), Size: aws.Int64(100)},
			},
		}, nil).
		Once()

	// For checkObjectAvailability - archived.
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(&s3.HeadObjectOutput{
			StorageClass: types.StorageClassGlacier,
		}, nil).
		Once()

	// For restoreObject.
	mockClient.EXPECT().
		RestoreObject(ctx, mock.Anything).
		Return(&s3.RestoreObjectOutput{}, nil).
		Once()

	err = reader.warmDirectory(ctx, testDir, types.TierStandard)

	require.NoError(t, err)
	assert.Len(t, reader.objectsToWarm, 1)
	assert.Equal(t, "test-dir/file1.txt", reader.objectsToWarm[0])
}

// TestReader_WarmDirectory_RestoringObject tests warming already restoring object.
func TestReader_WarmDirectory_RestoringObject(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	reader, err := NewReader(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)
	require.NoError(t, err)
	reader.objectsToWarm = make([]string, 0)

	// For ListObjects.
	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt"), Size: aws.Int64(100)},
			},
		}, nil).
		Once()

	// For checkObjectAvailability - restoring.
	restoreStatus := restoreValueOngoing
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(&s3.HeadObjectOutput{
			Restore: &restoreStatus,
		}, nil).
		Once()

	err = reader.warmDirectory(ctx, testDir, types.TierStandard)

	require.NoError(t, err)
	assert.Len(t, reader.objectsToWarm, 1)
}

// TestReader_WarmDirectory_Error tests error in warmDirectory.
func TestReader_WarmDirectory_Error(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	reader, err := NewReader(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)
	require.NoError(t, err)

	// For ListObjects - error.
	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(nil, errors.New("list failed")).
		Once()

	err = reader.warmDirectory(ctx, testDir, types.TierStandard)

	assert.Error(t, err)
}

// TestReader_CheckWarm_EmptyQueue tests checkWarm with empty queue.
func TestReader_CheckWarm_EmptyQueue(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	reader, err := NewReader(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)
	require.NoError(t, err)
	reader.objectsToWarm = []string{}

	err = reader.checkWarm(ctx)

	require.NoError(t, err)
}

// TestReader_PollWarmDirStatus_Success tests successful polling.
func TestReader_PollWarmDirStatus_Success(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	opts := func(o *options.Options) {
		o.PathList = []string{testFile}
		o.PollWarmDuration = 10 * time.Millisecond
	}

	reader, err := NewReader(ctx, mockClient, testBucket, opts)
	require.NoError(t, err)

	// First poll - still restoring.
	restoreOngoing := restoreValueOngoing
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(&s3.HeadObjectOutput{
			Restore: &restoreOngoing,
		}, nil).
		Once()

	// Second poll - now available.
	restoreFinished := restoreValueFinished
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(&s3.HeadObjectOutput{
			Restore: &restoreFinished,
		}, nil).
		Once()

	err = reader.pollWarmDirStatus(ctx, testFile)

	require.NoError(t, err)
}

// TestReader_PollWarmDirStatus_ContextCancelled tests context cancellation.
func TestReader_PollWarmDirStatus_ContextCancelled(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx, cancel := context.WithCancel(t.Context())

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	opts := func(o *options.Options) {
		o.PathList = []string{testFile}
		o.PollWarmDuration = 10 * time.Millisecond
	}

	reader, err := NewReader(ctx, mockClient, testBucket, opts)
	require.NoError(t, err)

	// Cancel context immediately.
	cancel()

	err = reader.pollWarmDirStatus(ctx, testFile)

	// Should return nil on context cancellation.
	require.NoError(t, err)
}

// TestReader_PollWarmDirStatus_Error tests error in polling.
func TestReader_PollWarmDirStatus_Error(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	opts := func(o *options.Options) {
		o.PathList = []string{testFile}
		o.PollWarmDuration = 10 * time.Millisecond
	}

	reader, err := NewReader(ctx, mockClient, testBucket, opts)
	require.NoError(t, err)

	// Polling returns error.
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(nil, errors.New("head object failed")).
		Once()

	err = reader.pollWarmDirStatus(ctx, testFile)

	assert.Error(t, err)
}

// TestReader_WarmStorage_RestoreFailed tests error in restore during warming.
func TestReader_WarmStorage_RestoreFailed(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	reader, err := NewReader(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)
	require.NoError(t, err)
	reader.PathList = []string{testDir}
	reader.objectsToWarm = make([]string, 0)

	// For ListObjects.
	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt"), Size: aws.Int64(100)},
			},
		}, nil).
		Once()

	// For checkObjectAvailability - archived.
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(&s3.HeadObjectOutput{
			StorageClass: types.StorageClassGlacier,
		}, nil).
		Once()

	// For restoreObject - fails.
	mockClient.EXPECT().
		RestoreObject(ctx, mock.Anything).
		Return(nil, errors.New("restore failed")).
		Once()

	err = reader.warmStorage(ctx, types.TierStandard)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to warm directory")
}

// TestReader_WarmStorage_MultipleFiles tests warming multiple files.
func TestReader_WarmStorage_MultipleFiles(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	reader, err := NewReader(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)
	require.NoError(t, err)
	reader.PathList = []string{testDir}
	reader.objectsToWarm = make([]string, 0)
	reader.PollWarmDuration = 10 * time.Millisecond

	// For ListObjects - return multiple files.
	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt"), Size: aws.Int64(100)},
				{Key: aws.String("test-dir/file2.txt"), Size: aws.Int64(200)},
			},
		}, nil).
		Once()

	// First file - archived, needs restore.
	mockClient.EXPECT().
		HeadObject(ctx, mock.MatchedBy(func(input *s3.HeadObjectInput) bool {
			return *input.Key == "test-dir/file1.txt"
		})).
		Return(&s3.HeadObjectOutput{
			StorageClass: types.StorageClassGlacier,
		}, nil).
		Once()

	mockClient.EXPECT().
		RestoreObject(ctx, mock.MatchedBy(func(input *s3.RestoreObjectInput) bool {
			return *input.Key == "test-dir/file1.txt"
		})).
		Return(&s3.RestoreObjectOutput{}, nil).
		Once()

	// Second file - already available.
	mockClient.EXPECT().
		HeadObject(ctx, mock.MatchedBy(func(input *s3.HeadObjectInput) bool {
			return *input.Key == "test-dir/file2.txt"
		})).
		Return(&s3.HeadObjectOutput{
			StorageClass: types.StorageClassStandard,
		}, nil).
		Once()

	// Polling for file1 - now available.
	restoreFinished := restoreValueFinished
	mockClient.EXPECT().
		HeadObject(ctx, mock.MatchedBy(func(input *s3.HeadObjectInput) bool {
			return *input.Key == "test-dir/file1.txt"
		})).
		Return(&s3.HeadObjectOutput{
			Restore: &restoreFinished,
		}, nil).
		Once()

	err = reader.warmStorage(ctx, types.TierStandard)

	require.NoError(t, err)
}

// TestReader_CheckWarm_PollError tests error in checkWarm polling.
func TestReader_CheckWarm_PollError(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	opts := func(o *options.Options) {
		o.PathList = []string{testFile}
		o.PollWarmDuration = 10 * time.Millisecond
	}

	reader, err := NewReader(ctx, mockClient, testBucket, opts)
	require.NoError(t, err)
	reader.objectsToWarm = []string{testFile}

	// Polling returns error
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(nil, errors.New("head object failed")).
		Once()

	err = reader.checkWarm(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to poll dir status")
}
