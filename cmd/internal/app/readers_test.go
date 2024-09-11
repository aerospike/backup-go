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

	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/stretchr/testify/assert"
)

func TestNewLocalReader(t *testing.T) {
	r := &models.Restore{}
	c := &models.Common{
		Directory: t.TempDir(),
	}

	reader, err := newLocalReader(r, c)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, "directory", reader.GetType())

	r = &models.Restore{
		InputFile: t.TempDir() + "/file.bak",
	}
	c = &models.Common{}

	reader, err = newLocalReader(r, c)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, "directory", reader.GetType())

	r = &models.Restore{}
	reader, err = newLocalReader(r, c)
	assert.Error(t, err)
	assert.Nil(t, reader)
}

func TestNewS3Reader(t *testing.T) {
	err := createAwsCredentials()
	assert.NoError(t, err)

	r := &models.Restore{}
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

	writer, err := newS3Reader(ctx, s3cfg, r, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, "s3", writer.GetType())

	r = &models.Restore{
		InputFile: "asbackup/" + t.TempDir() + "/file.bak",
	}
	c = &models.Common{}

	writer, err = newS3Reader(ctx, s3cfg, r, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, "s3", writer.GetType())
}
