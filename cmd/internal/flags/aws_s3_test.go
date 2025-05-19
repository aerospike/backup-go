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

package flags

import (
	"testing"

	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/stretchr/testify/assert"
)

func TestAwsS3_NewFlagSet(t *testing.T) {
	t.Parallel()
	awsS3 := NewAwsS3(OperationBackup)

	flagSet := awsS3.NewFlagSet()

	args := []string{
		"--s3-region", "us-west-2",
		"--s3-profile", "my-profile",
		"--s3-endpoint-override", "https://s3.custom-endpoint.com",
		"--s3-access-key-id", "my-access-key-id",
		"--s3-secret-access-key", "my-secret-access-key",
		"--s3-storage-class", "my-storage-class",
		"--s3-chunk-size", "1",
		"--s3-retry-max-attempts", "10",
		"--s3-retry-max-backoff", "10",
		"--s3-retry-backoff", "10",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	result := awsS3.GetAwsS3()

	assert.Equal(t, "us-west-2", result.Region, "The s3-region flag should be parsed correctly")
	assert.Equal(t, "my-profile", result.Profile, "The s3-profile flag should be parsed correctly")
	assert.Equal(t, "https://s3.custom-endpoint.com", result.Endpoint, "The s3-endpoint-override flag should be parsed correctly")
	assert.Equal(t, "my-access-key-id", result.AccessKeyID, "The s3-access-key-id flag should be parsed correctly")
	assert.Equal(t, "my-secret-access-key", result.SecretAccessKey, "The s3-secret-access-key flag should be parsed correctly")
	assert.Equal(t, "my-storage-class", result.StorageClass, "The s3-storage-class flag should be parsed correctly")
	assert.Equal(t, 1, result.ChunkSize, "The s3-chunk-size flag should be parsed correctly")
	assert.Equal(t, 10, result.RetryMaxAttempts, "The s3-retry-max-attempts flag should be parsed correctly")
	assert.Equal(t, 10, result.RetryMaxBackoffSeconds, "The s3-retry-max-backoff flag should be parsed correctly")
	assert.Equal(t, 10, result.RetryBackoffSeconds, "The s3-retry-backoff flag should be parsed correctly")
}

func TestAwsS3_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()
	awsS3 := NewAwsS3(OperationBackup)

	flagSet := awsS3.NewFlagSet()

	err := flagSet.Parse([]string{})
	assert.NoError(t, err)

	result := awsS3.GetAwsS3()

	assert.Equal(t, "", result.Region, "The default value for s3-region should be an empty string")
	assert.Equal(t, "", result.Profile, "The default value for s3-profile should be 'default'")
	assert.Equal(t, "", result.Endpoint, "The default value for s3-endpoint-override should be an empty string")
	assert.Equal(t, "", result.AccessKeyID, "The default value for s3-access-key-id should be an empty string")
	assert.Equal(t, "", result.SecretAccessKey, "The default value for s3-secret-access-key should be an empty string")
	assert.Equal(t, "", result.StorageClass, "The default value for s3-storage-class should be an empty string")
	assert.Equal(t, models.DefaultChunkSize, result.ChunkSize, "The default value for s3-chunk-size should be 5mb")
	assert.Equal(t, 100, result.RetryMaxAttempts, "The default value for s3-retry-max-attempts should be ")
	assert.Equal(t, 90, result.RetryMaxBackoffSeconds, "The default value for s3-retry-max-backoff should be ")
	assert.Equal(t, 60, result.RetryBackoffSeconds, "The default value for s3-retry-backoff should be ")
}
