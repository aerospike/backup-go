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
	b := &models.Backup{
		RemoveFiles: true,
	}
	c := &models.Common{
		Directory: t.TempDir(),
	}

	writer, err := newLocalWriter(b, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, "directory", writer.GetType())

	b = &models.Backup{
		OutputFile: t.TempDir() + "/file.bak",
	}
	c = &models.Common{}

	writer, err = newLocalWriter(b, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, "directory", writer.GetType())

	b = &models.Backup{}
	writer, err = newLocalWriter(b, c)
	assert.Error(t, err)
	assert.Nil(t, writer)
}

func TestNewS3Writer(t *testing.T) {
	err := createAwsCredentials()
	assert.NoError(t, err)

	b := &models.Backup{
		RemoveFiles: true,
	}
	c := &models.Common{
		Directory: "asbackup/" + t.TempDir(),
	}

	s3cfg := &models.AwsS3{
		Region:      "eu",
		Profile:     "minio",
		Endpoint:    "http://localhost:9000",
		MinPartSize: 10,
	}

	ctx := context.Background()

	writer, err := newS3Writer(ctx, s3cfg, b, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, "s3", writer.GetType())

	b = &models.Backup{
		OutputFile:  "asbackup/" + t.TempDir() + "/file.bak",
		RemoveFiles: true,
	}
	c = &models.Common{}

	writer, err = newS3Writer(ctx, s3cfg, b, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, "s3", writer.GetType())
}

func createAwsCredentials() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("error getting home directory: %v", err)
	}

	awsDir := filepath.Join(home, ".aws")
	err = os.MkdirAll(awsDir, 0o700)
	if err != nil {
		return fmt.Errorf("error creating .aws directory: %v", err)
	}

	filePath := filepath.Join(awsDir, "credentials")

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		credentialsFileBytes := []byte(`[minio]
aws_access_key_id = minioadmin
aws_secret_access_key = minioadminpassword`)

		err = os.WriteFile(filePath, credentialsFileBytes, 0o600)
		if err != nil {
			return fmt.Errorf("error writing ~/.aws/credentials file: %v", err)
		}

		fmt.Println("Credentials file created successfully!")
	}

	return nil
}
