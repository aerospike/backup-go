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
	"testing"

	"github.com/aerospike/backup-go/cmd/asbackup/models"
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
	storage := &models.Storage{
		Directory:   t.TempDir(),
		RemoveFiles: true,
	}

	writer, err := newLocalWriter(storage)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, "directory", writer.GetType())

	storage = &models.Storage{
		OutputFile: t.TempDir() + "/file.bak",
	}

	writer, err = newLocalWriter(storage)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, "directory", writer.GetType())

	storage = &models.Storage{}
	writer, err = newLocalWriter(storage)
	assert.Error(t, err)
	assert.Nil(t, writer)
}

func TestNewS3Writer(t *testing.T) {
	storage := &models.Storage{
		Directory:   "as-backup-bucket/" + t.TempDir(),
		RemoveFiles: true,
	}

	s3cfg := &models.AwsS3{
		Region:      "eu",
		Profile:     "minio",
		Endpoint:    "http://localhost:9000",
		MinPartSize: 10,
	}

	ctx := context.Background()

	writer, err := newS3Writer(ctx, s3cfg, storage)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, "s3", writer.GetType())

	storage = &models.Storage{
		OutputFile:  "as-backup-bucket/" + t.TempDir() + "/file.bak",
		RemoveFiles: true,
	}

	writer, err = newS3Writer(ctx, s3cfg, storage)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, "s3", writer.GetType())
}
