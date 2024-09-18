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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/stretchr/testify/assert"
)

const (
	testBucket    = "test"
	testProjectID = "test-id"
	testFileName  = "/file.bak"

	testLocalType = "directory"

	testS3Endpoint = "http://localhost:9000"
	testS3Region   = "eu"
	testS3Profile  = "minio"
	testS3Type     = "s3"

	testGcpEndpoint = "http://127.0.0.1:4443/storage/v1/b"
	testGcpType     = "gcp-storage"

	testAzureEndpoint    = "http://127.0.0.1:5000/devstoreaccount1"
	testAzureAccountName = "devstoreaccount1"
	testAzureAccountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	testAzureType        = "azure-blob"
)

func TestGetBucketFromPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input          string
		expectedBucket string
		expectedPath   string
	}{
		{input: "bucket/path/to/file", expectedBucket: "bucket", expectedPath: "path/to/file"},
		{input: "single-part", expectedBucket: "", expectedPath: "single-part"},
		{input: "bucket/", expectedBucket: "bucket", expectedPath: ""},
		{input: "", expectedBucket: "", expectedPath: ""},
	}

	for _, test := range tests {
		bucket, path := getBucketFromPath(test.input)
		assert.Equal(t, test.expectedBucket, bucket)
		assert.Equal(t, test.expectedPath, path)
	}
}

func TestNewLocalWriter(t *testing.T) {
	t.Parallel()
	b := &models.Backup{
		RemoveFiles: true,
	}
	c := &models.Common{
		Directory: t.TempDir(),
	}
	ctx := context.Background()
	writer, err := newLocalWriter(ctx, b, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testLocalType, writer.GetType())

	b = &models.Backup{
		OutputFile: t.TempDir() + testFileName,
	}
	c = &models.Common{}

	writer, err = newLocalWriter(ctx, b, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testLocalType, writer.GetType())

	b = &models.Backup{}
	writer, err = newLocalWriter(ctx, b, c)
	assert.Error(t, err)
	assert.Nil(t, writer)
}

func TestNewS3Writer(t *testing.T) {
	t.Parallel()
	err := createAwsCredentials()
	assert.NoError(t, err)

	b := &models.Backup{
		RemoveFiles: true,
	}
	c := &models.Common{
		Directory: "asbackup/" + t.TempDir(),
	}

	s3cfg := &models.AwsS3{
		Region:   testS3Region,
		Profile:  testS3Profile,
		Endpoint: testS3Endpoint,
	}

	ctx := context.Background()

	writer, err := newS3Writer(ctx, s3cfg, b, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testS3Type, writer.GetType())

	b = &models.Backup{
		OutputFile:  "asbackup/" + t.TempDir() + testFileName,
		RemoveFiles: true,
	}
	c = &models.Common{}

	writer, err = newS3Writer(ctx, s3cfg, b, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testS3Type, writer.GetType())
}

func createAwsCredentials() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("error getting home directory: %w", err)
	}

	awsDir := filepath.Join(home, ".aws")
	err = os.MkdirAll(awsDir, 0o700)
	if err != nil {
		return fmt.Errorf("error creating .aws directory: %w", err)
	}

	filePath := filepath.Join(awsDir, "credentials")

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		credentialsFileBytes := []byte(`[minio]
aws_access_key_id = minioadmin
aws_secret_access_key = minioadminpassword`)

		err = os.WriteFile(filePath, credentialsFileBytes, 0o600)
		if err != nil {
			return fmt.Errorf("error writing ~/.aws/credentials file: %w", err)
		}

		fmt.Println("Credentials file created successfully!")
	}

	return nil
}

func TestGcpWriter(t *testing.T) {
	t.Parallel()
	err := createGcpBucket()
	assert.NoError(t, err)

	b := &models.Backup{
		RemoveFiles: true,
	}
	c := &models.Common{
		Directory: t.TempDir(),
	}

	cfg := &models.GcpStorage{
		BucketName: testBucket,
		Endpoint:   testGcpEndpoint,
	}

	ctx := context.Background()

	writer, err := newGcpWriter(ctx, cfg, b, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testGcpType, writer.GetType())

	b = &models.Backup{
		OutputFile:  t.TempDir() + testFileName,
		RemoveFiles: true,
	}
	c = &models.Common{}

	writer, err = newGcpWriter(ctx, cfg, b, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testGcpType, writer.GetType())
}

func createGcpBucket() error {
	ctx := context.Background()
	cfg := &models.GcpStorage{
		BucketName: testBucket,
		Endpoint:   testGcpEndpoint,
	}
	c, err := newGcpClient(ctx, cfg)
	if err != nil {
		return err
	}

	bucket := c.Bucket(testBucket)
	_ = bucket.Create(ctx, testProjectID, nil)

	return nil
}

func TestAzureWriter(t *testing.T) {
	t.Parallel()
	err := createAzureContainer()
	assert.NoError(t, err)

	b := &models.Backup{
		RemoveFiles: true,
	}
	c := &models.Common{
		Directory: t.TempDir(),
	}

	cfg := &models.AzureBlob{
		AccountName:   testAzureAccountName,
		AccountKey:    testAzureAccountKey,
		Endpoint:      testAzureEndpoint,
		ContainerName: testBucket,
	}

	ctx := context.Background()

	writer, err := newAzureWriter(ctx, cfg, b, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testAzureType, writer.GetType())

	b = &models.Backup{
		OutputFile:  t.TempDir() + testFileName,
		RemoveFiles: true,
	}
	c = &models.Common{}

	writer, err = newAzureWriter(ctx, cfg, b, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testAzureType, writer.GetType())
}

func createAzureContainer() error {
	cfg := &models.AzureBlob{
		AccountName:   testAzureAccountName,
		AccountKey:    testAzureAccountKey,
		Endpoint:      testAzureEndpoint,
		ContainerName: testBucket,
	}

	c, err := newAzureClient(cfg)
	if err != nil {
		return err
	}

	ctx := context.Background()

	_, _ = c.CreateContainer(ctx, testBucket, nil)

	return nil
}
