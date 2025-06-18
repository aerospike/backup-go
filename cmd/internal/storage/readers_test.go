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
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	appConfig "github.com/aerospike/backup-go/cmd/internal/config"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testS3Bucket     = "asbackup"
	testFileNameASBX = "0_test_1.asbx"
)

func TestNewLocalReader(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	dir := t.TempDir()

	params := &appConfig.RestoreParams{
		Restore: &models.Restore{
			Common: models.Common{
				Directory: dir,
			},
		},
		AwsS3:      &models.AwsS3{},
		GcpStorage: &models.GcpStorage{},
		AzureBlob:  &models.AzureBlob{},
	}

	err := createTmpFileLocal(dir, testFileNameASBX)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	reader, err := newReader(ctx, params, nil, true, logger)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, testLocalType, reader.GetType())

	params = &appConfig.RestoreParams{
		Restore: &models.Restore{
			InputFile: dir + testFileNameASBX,
		},
		AwsS3:      &models.AwsS3{},
		GcpStorage: &models.GcpStorage{},
		AzureBlob:  &models.AzureBlob{},
	}

	reader, err = newReader(ctx, params, nil, false, logger)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, testLocalType, reader.GetType())

	params = &appConfig.RestoreParams{
		Restore:    &models.Restore{},
		AwsS3:      &models.AwsS3{},
		GcpStorage: &models.GcpStorage{},
		AzureBlob:  &models.AzureBlob{},
	}
	reader, err = newReader(ctx, params, nil, false, logger)
	assert.Error(t, err)
	assert.Nil(t, reader)
}

func createTmpFileLocal(dir, fileName string) error {
	filePath := filepath.Join(dir, fileName)
	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	_ = f.Close()

	return nil
}

func TestNewS3Reader(t *testing.T) {
	t.Parallel()
	err := createAwsCredentials()
	assert.NoError(t, err)

	dir := t.TempDir()
	dir = strings.TrimPrefix(dir, "/")

	params := &appConfig.RestoreParams{
		Restore: &models.Restore{
			Common: models.Common{
				Directory: dir,
			},
		},
		AwsS3: &models.AwsS3{
			BucketName:          testS3Bucket,
			Region:              testS3Region,
			Profile:             testS3Profile,
			Endpoint:            testS3Endpoint,
			AccessTier:          "Standard",
			RestorePollDuration: 10000,
		},
		GcpStorage: &models.GcpStorage{},
		AzureBlob:  &models.AzureBlob{},
	}

	ctx := context.Background()
	client, err := newS3Client(ctx, params.AwsS3)
	require.NoError(t, err)
	err = createTmpFileS3(ctx, client, dir, testFileNameASBX)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	reader, err := newReader(ctx, params, nil, true, logger)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, testS3Type, reader.GetType())

	params = &appConfig.RestoreParams{
		Restore: &models.Restore{
			InputFile: dir + testFileName,
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

	reader, err = newReader(ctx, params, nil, false, logger)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, testS3Type, reader.GetType())
}

func createTmpFileS3(ctx context.Context, client *s3.Client, dir, fileName string) error {
	fileName = filepath.Join(dir, fileName)
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(testS3Bucket),
		Key:    aws.String(fileName),
		Body:   bytes.NewReader([]byte("test")),
	}); err != nil {
		return err
	}

	return nil
}

func TestNewGcpReader(t *testing.T) {
	t.Parallel()
	err := createGcpBucket()
	assert.NoError(t, err)

	dir := t.TempDir()
	dir = strings.TrimPrefix(dir, "/")

	params := &appConfig.RestoreParams{
		Restore: &models.Restore{
			Common: models.Common{
				Directory: dir,
			},
		},
		GcpStorage: &models.GcpStorage{
			BucketName: testBucket,
			Endpoint:   testGcpEndpoint,
		},
		AwsS3:     &models.AwsS3{},
		AzureBlob: &models.AzureBlob{},
	}

	ctx := context.Background()
	client, err := newGcpClient(ctx, params.GcpStorage)
	require.NoError(t, err)
	err = createTmpFileGcp(ctx, client, dir, testFileNameASBX)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	reader, err := newReader(ctx, params, nil, true, logger)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, testGcpType, reader.GetType())

	params = &appConfig.RestoreParams{
		Restore: &models.Restore{
			InputFile: dir + testFileName,
		},
		GcpStorage: &models.GcpStorage{
			BucketName: testBucket,
			Endpoint:   testGcpEndpoint,
		},
		AwsS3:     &models.AwsS3{},
		AzureBlob: &models.AzureBlob{},
	}

	reader, err = newReader(ctx, params, nil, false, logger)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, testGcpType, reader.GetType())
}

func createTmpFileGcp(ctx context.Context, client *storage.Client, dir, fileName string) error {
	fileName = filepath.Join(dir, fileName)
	sw := client.Bucket(testBucket).Object(fileName).NewWriter(ctx)
	if _, err := sw.Write([]byte("test")); err != nil {
		return err
	}
	if err := sw.Close(); err != nil {
		return err
	}

	return nil
}

func TestNewAzureReader(t *testing.T) {
	t.Parallel()
	err := createAzureContainer()
	assert.NoError(t, err)

	dir := t.TempDir()
	dir = strings.TrimPrefix(dir, "/")

	params := &appConfig.RestoreParams{
		Restore: &models.Restore{
			Common: models.Common{
				Directory: dir,
			},
		},
		AzureBlob: &models.AzureBlob{
			AccountName:         testAzureAccountName,
			AccountKey:          testAzureAccountKey,
			Endpoint:            testAzureEndpoint,
			ContainerName:       testBucket,
			AccessTier:          "Cold",
			RestorePollDuration: 10000,
		},
		AwsS3:      &models.AwsS3{},
		GcpStorage: &models.GcpStorage{},
	}

	ctx := context.Background()
	client, err := newAzureClient(params.AzureBlob)
	require.NoError(t, err)
	err = createTmpFileAzure(ctx, client, dir, testFileNameASBX)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	reader, err := newReader(ctx, params, nil, true, logger)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, testAzureType, reader.GetType())

	params = &appConfig.RestoreParams{
		Restore: &models.Restore{
			InputFile: dir + testFileName,
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

	reader, err = newReader(ctx, params, nil, false, logger)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, testAzureType, reader.GetType())
}

func createTmpFileAzure(ctx context.Context, client *azblob.Client, dir, fileName string) error {
	fileName = filepath.Join(dir, fileName)
	if _, err := client.UploadStream(ctx, testBucket, fileName, strings.NewReader("test"), nil); err != nil {
		return err
	}

	return nil
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
