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

package app

import (
	"context"
	"path"
	"testing"

	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/stretchr/testify/assert"
)

const testS3Bucket = "asbackup"

func TestNewLocalReader(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	params := &ASRestoreParams{
		RestoreParams: &models.Restore{},
		CommonParams: &models.Common{
			Directory: t.TempDir(),
		},
		AwsS3:      &models.AwsS3{},
		GcpStorage: &models.GcpStorage{},
		AzureBlob:  &models.AzureBlob{},
	}

	reader, err := newReader(ctx, params, nil, true)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, testLocalType, reader.GetType())

	params = &ASRestoreParams{
		RestoreParams: &models.Restore{
			InputFile: t.TempDir() + testFileName,
		},
		CommonParams: &models.Common{},
		AwsS3:        &models.AwsS3{},
		GcpStorage:   &models.GcpStorage{},
		AzureBlob:    &models.AzureBlob{},
	}

	reader, err = newReader(ctx, params, nil, false)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, testLocalType, reader.GetType())

	params = &ASRestoreParams{
		RestoreParams: &models.Restore{},
		CommonParams:  &models.Common{},
		AwsS3:         &models.AwsS3{},
		GcpStorage:    &models.GcpStorage{},
		AzureBlob:     &models.AzureBlob{},
	}
	reader, err = newReader(ctx, params, nil, false)
	assert.Error(t, err)
	assert.Nil(t, reader)
}

func TestNewS3Reader(t *testing.T) {
	t.Parallel()
	err := createAwsCredentials()
	assert.NoError(t, err)

	params := &ASRestoreParams{
		RestoreParams: &models.Restore{},
		CommonParams: &models.Common{
			Directory: t.TempDir(),
		},
		AwsS3: &models.AwsS3{
			BucketName: testS3Bucket,
			Region:     testS3Region,
			Profile:    testS3Profile,
			Endpoint:   testS3Endpoint,
		},
		GcpStorage: &models.GcpStorage{},
		AzureBlob:  &models.AzureBlob{},
	}

	ctx := context.Background()

	reader, err := newReader(ctx, params, nil, true)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, testS3Type, reader.GetType())

	params = &ASRestoreParams{
		RestoreParams: &models.Restore{
			InputFile: t.TempDir() + testFileName,
		},
		CommonParams: &models.Common{},
		AwsS3: &models.AwsS3{
			BucketName: testS3Bucket,
			Region:     testS3Region,
			Profile:    testS3Profile,
			Endpoint:   testS3Endpoint,
		},
		GcpStorage: &models.GcpStorage{},
		AzureBlob:  &models.AzureBlob{},
	}

	reader, err = newReader(ctx, params, nil, false)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, testS3Type, reader.GetType())
}

func TestNewGcpReader(t *testing.T) {
	t.Parallel()
	err := createGcpBucket()
	assert.NoError(t, err)

	params := &ASRestoreParams{
		RestoreParams: &models.Restore{},
		CommonParams: &models.Common{
			Directory: t.TempDir(),
		},
		GcpStorage: &models.GcpStorage{
			BucketName: testBucket,
			Endpoint:   testGcpEndpoint,
		},
		AwsS3:     &models.AwsS3{},
		AzureBlob: &models.AzureBlob{},
	}

	ctx := context.Background()

	reader, err := newReader(ctx, params, nil, true)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, testGcpType, reader.GetType())

	params = &ASRestoreParams{
		RestoreParams: &models.Restore{
			InputFile: t.TempDir() + testFileName,
		},
		CommonParams: &models.Common{},
		GcpStorage: &models.GcpStorage{
			BucketName: testBucket,
			Endpoint:   testGcpEndpoint,
		},
		AwsS3:     &models.AwsS3{},
		AzureBlob: &models.AzureBlob{},
	}

	reader, err = newReader(ctx, params, nil, false)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, testGcpType, reader.GetType())
}

func TestNewAzureReader(t *testing.T) {
	t.Parallel()
	err := createAzureContainer()
	assert.NoError(t, err)

	params := &ASRestoreParams{
		RestoreParams: &models.Restore{},
		CommonParams: &models.Common{
			Directory: t.TempDir(),
		},
		AzureBlob: &models.AzureBlob{
			AccountName:   testAzureAccountName,
			AccountKey:    testAzureAccountKey,
			Endpoint:      testAzureEndpoint,
			ContainerName: testBucket,
		},
		AwsS3:      &models.AwsS3{},
		GcpStorage: &models.GcpStorage{},
	}

	ctx := context.Background()

	reader, err := newReader(ctx, params, nil, true)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, testAzureType, reader.GetType())

	params = &ASRestoreParams{
		RestoreParams: &models.Restore{
			InputFile: t.TempDir() + testFileName,
		},
		CommonParams: &models.Common{},
		AzureBlob: &models.AzureBlob{
			AccountName:   testAzureAccountName,
			AccountKey:    testAzureAccountKey,
			Endpoint:      testAzureEndpoint,
			ContainerName: testBucket,
		},
		AwsS3:      &models.AwsS3{},
		GcpStorage: &models.GcpStorage{},
	}

	reader, err = newReader(ctx, params, nil, false)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, testAzureType, reader.GetType())
}

func TestPrepareDirectoryList(t *testing.T) {
	tests := []struct {
		name      string
		parentDir string
		dirList   string
		expected  []string
	}{
		{
			name:      "Empty input",
			parentDir: "",
			dirList:   "",
			expected:  nil,
		},
		{
			name:      "Single directory without parentDir",
			parentDir: "",
			dirList:   "dir1",
			expected:  []string{"dir1"},
		},
		{
			name:      "Multiple directories without parentDir",
			parentDir: "",
			dirList:   "dir1,dir2,dir3",
			expected:  []string{"dir1", "dir2", "dir3"},
		},
		{
			name:      "Single directory with parentDir",
			parentDir: "parent",
			dirList:   "dir1",
			expected:  []string{path.Join("parent", "dir1")},
		},
		{
			name:      "Multiple directories with parentDir",
			parentDir: "parent",
			dirList:   "dir1,dir2,dir3",
			expected: []string{
				path.Join("parent", "dir1"),
				path.Join("parent", "dir2"),
				path.Join("parent", "dir3"),
			},
		},
		{
			name:      "Trailing commas in dirList",
			parentDir: "parent",
			dirList:   "dir1,dir2,",
			expected: []string{
				path.Join("parent", "dir1"),
				path.Join("parent", "dir2"),
				path.Join("parent", ""),
			},
		},
		{
			name:      "Whitespace in dirList",
			parentDir: "parent",
			dirList:   " dir1 , dir2 ,dir3 ",
			expected: []string{
				path.Join("parent", " dir1 "),
				path.Join("parent", " dir2 "),
				path.Join("parent", "dir3 "),
			},
		},
		{
			name:      "ParentDir is empty but dirList has valid directories",
			parentDir: "",
			dirList:   "dir1,dir2",
			expected:  []string{"dir1", "dir2"},
		},
		{
			name:      "ParentDir has trailing slash",
			parentDir: "parent/",
			dirList:   "dir1,dir2",
			expected: []string{
				path.Join("parent/", "dir1"),
				path.Join("parent/", "dir2"),
			},
		},
		{
			name:      "ParentDir with absolute path",
			parentDir: "/absolute/path",
			dirList:   "dir1,dir2",
			expected: []string{
				path.Join("/absolute/path", "dir1"),
				path.Join("/absolute/path", "dir2"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := prepareDirectoryList(tt.parentDir, tt.dirList)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
