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
	assert.Equal(t, testLocalType, reader.GetType())

	r = &models.Restore{
		InputFile: t.TempDir() + testFileName,
	}
	c = &models.Common{}

	reader, err = newLocalReader(r, c)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Equal(t, testLocalType, reader.GetType())

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
		Region:      testS3Region,
		Profile:     testS3Profile,
		Endpoint:    testS3Endpoint,
		MinPartSize: 10,
	}

	ctx := context.Background()

	writer, err := newS3Reader(ctx, s3cfg, r, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testS3Type, writer.GetType())

	r = &models.Restore{
		InputFile: "asbackup/" + t.TempDir() + testFileName,
	}
	c = &models.Common{}

	writer, err = newS3Reader(ctx, s3cfg, r, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testS3Type, writer.GetType())
}

func TestNewGcpReader(t *testing.T) {
	err := createGcpBucket()
	assert.NoError(t, err)

	r := &models.Restore{}
	c := &models.Common{
		Directory: t.TempDir(),
	}

	cfg := &models.GcpStorage{
		BucketName: testBucket,
		Endpoint:   testGcpEndpoint,
	}

	ctx := context.Background()

	writer, err := newGcpReader(ctx, cfg, r, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testGcpType, writer.GetType())

	r = &models.Restore{
		InputFile: t.TempDir() + testFileName,
	}
	c = &models.Common{}

	writer, err = newGcpReader(ctx, cfg, r, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testGcpType, writer.GetType())
}

func TestNewAzureReader(t *testing.T) {
	err := createAzureContainer()
	assert.NoError(t, err)

	r := &models.Restore{}
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

	writer, err := newAzureReader(ctx, cfg, r, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testAzureType, writer.GetType())

	r = &models.Restore{
		InputFile: t.TempDir() + testFileName,
	}
	c = &models.Common{}

	writer, err = newAzureReader(ctx, cfg, r, c)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testAzureType, writer.GetType())
}
