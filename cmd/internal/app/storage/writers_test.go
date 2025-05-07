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
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/aerospike/backup-go/cmd/internal/app/config"
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

	testAzureEndpoint    = "http://127.0.0.1:10000/devstoreaccount1"
	testAzureAccountName = "devstoreaccount1"
	testAzureAccountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	testAzureType        = "azure-blob"
)

func TestNewLocalWriter(t *testing.T) {
	t.Parallel()

	params := &config.BackupParams{
		Backup: &models.Backup{
			RemoveFiles: true,
		},
		Common: &models.Common{
			Directory: t.TempDir(),
		},
		AwsS3:      &models.AwsS3{},
		GcpStorage: &models.GcpStorage{},
		AzureBlob:  &models.AzureBlob{},
	}
	ctx := context.Background()
	writer, err := newWriter(ctx, params, nil, slog.Default())
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testLocalType, writer.GetType())

	params = &config.BackupParams{
		Backup: &models.Backup{
			OutputFile: t.TempDir() + testFileName,
		},
		Common:     &models.Common{},
		AwsS3:      &models.AwsS3{},
		GcpStorage: &models.GcpStorage{},
		AzureBlob:  &models.AzureBlob{},
	}
	writer, err = newWriter(ctx, params, nil, slog.Default())
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testLocalType, writer.GetType())

	params = &config.BackupParams{
		Backup:     &models.Backup{},
		Common:     &models.Common{},
		AwsS3:      &models.AwsS3{},
		GcpStorage: &models.GcpStorage{},
		AzureBlob:  &models.AzureBlob{},
	}
	writer, err = newWriter(ctx, params, nil, slog.Default())
	assert.Error(t, err)
	assert.Nil(t, writer)
}

func TestNewS3Writer(t *testing.T) {
	t.Parallel()
	err := createAwsCredentials()
	assert.NoError(t, err)

	params := &config.BackupParams{
		Backup: &models.Backup{
			RemoveFiles: true,
		},
		Common: &models.Common{
			Directory: t.TempDir(),
		},
		AwsS3: &models.AwsS3{
			BucketName:   testS3Bucket,
			Region:       testS3Region,
			Profile:      testS3Profile,
			Endpoint:     testS3Endpoint,
			StorageClass: "STANDARD",
		},
		GcpStorage: &models.GcpStorage{},
		AzureBlob:  &models.AzureBlob{},
	}

	ctx := context.Background()

	writer, err := newWriter(ctx, params, nil, slog.Default())
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testS3Type, writer.GetType())

	params = &config.BackupParams{
		Backup: &models.Backup{
			OutputFile: t.TempDir() + testFileName,
		},
		Common: &models.Common{},
		AwsS3: &models.AwsS3{
			BucketName: testS3Bucket,
			Region:     testS3Region,
			Profile:    testS3Profile,
			Endpoint:   testS3Endpoint,
		},
		GcpStorage: &models.GcpStorage{},
		AzureBlob:  &models.AzureBlob{},
	}

	writer, err = newWriter(ctx, params, nil, slog.Default())
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

	params := &config.BackupParams{
		Backup: &models.Backup{
			RemoveFiles: true,
		},
		Common: &models.Common{
			Directory: t.TempDir(),
		},
		GcpStorage: &models.GcpStorage{
			BucketName: testBucket,
			Endpoint:   testGcpEndpoint,
		},
		AzureBlob: &models.AzureBlob{},
		AwsS3:     &models.AwsS3{},
	}

	ctx := context.Background()

	writer, err := newWriter(ctx, params, nil, slog.Default())
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testGcpType, writer.GetType())

	params = &config.BackupParams{
		Backup: &models.Backup{
			OutputFile: t.TempDir() + testFileName,
		},
		Common: &models.Common{},
		GcpStorage: &models.GcpStorage{
			BucketName: testBucket,
			Endpoint:   testGcpEndpoint,
		},
		AzureBlob: &models.AzureBlob{},
		AwsS3:     &models.AwsS3{},
	}

	writer, err = newWriter(ctx, params, nil, slog.Default())
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

	params := &config.BackupParams{
		Backup: &models.Backup{
			RemoveFiles: true,
		},
		Common: &models.Common{
			Directory: t.TempDir(),
		},
		AzureBlob: &models.AzureBlob{
			AccountName:   testAzureAccountName,
			AccountKey:    testAzureAccountKey,
			Endpoint:      testAzureEndpoint,
			ContainerName: testBucket,
			AccessTier:    "Cold",
		},
		GcpStorage: &models.GcpStorage{},
		AwsS3:      &models.AwsS3{},
	}

	ctx := context.Background()

	writer, err := newWriter(ctx, params, nil, slog.Default())
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testAzureType, writer.GetType())

	params = &config.BackupParams{
		Backup: &models.Backup{
			OutputFile: t.TempDir() + testFileName,
		},
		Common: &models.Common{},
		AzureBlob: &models.AzureBlob{
			AccountName:   testAzureAccountName,
			AccountKey:    testAzureAccountKey,
			Endpoint:      testAzureEndpoint,
			ContainerName: testBucket,
		},
		GcpStorage: &models.GcpStorage{},
		AwsS3:      &models.AwsS3{},
	}

	writer, err = newWriter(ctx, params, nil, slog.Default())
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
