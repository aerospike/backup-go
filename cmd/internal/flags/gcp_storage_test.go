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

func TestGcpStorage_NewFlagSet(t *testing.T) {
	t.Parallel()
	gcpStorage := NewGcpStorage(OperationBackup)

	flagSet := gcpStorage.NewFlagSet()

	args := []string{
		"--gcp-key-path", "/path/to/keyfile.json",
		"--gcp-bucket-name", "my-bucket",
		"--gcp-endpoint-override", "https://gcp.custom-endpoint.com",
		"--gcp-chunk-size", "1",
		"--gcp-retry-max-attempts", "10",
		"--gcp-retry-max-backoff", "10",
		"--gcp-retry-init-backoff", "10",
		"--gcp-retry-backoff-multiplier", "10",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	result := gcpStorage.GetGcpStorage()

	assert.Equal(t, "/path/to/keyfile.json", result.KeyFile, "The gcp-key-path flag should be parsed correctly")
	assert.Equal(t, "my-bucket", result.BucketName, "The gcp-bucket-name flag should be parsed correctly")
	assert.Equal(t, "https://gcp.custom-endpoint.com", result.Endpoint, "The gcp-endpoint-override flag should be parsed correctly")
	assert.Equal(t, 1, result.ChunkSize, "The gcp-chunk-size flag should be parsed correctly")
	assert.Equal(t, 10, result.RetryMaxAttempts, "The gcp-retry-max-attempts flag should be parsed correctly")
	assert.Equal(t, 10, result.RetryBackoffMaxSeconds, "The gcp-retry-max-backoff flag should be parsed correctly")
	assert.Equal(t, 10, result.RetryBackoffInitSeconds, "The gcp-retry-init-backoff flag should be parsed correctly")
	assert.Equal(t, float64(10), result.RetryBackoffMultiplier, "The gcp-retry-backoff-multiplier flag should be parsed correctly")
}

func TestGcpStorage_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()
	gcpStorage := NewGcpStorage(OperationBackup)

	flagSet := gcpStorage.NewFlagSet()

	err := flagSet.Parse([]string{})
	assert.NoError(t, err)

	result := gcpStorage.GetGcpStorage()

	assert.Equal(t, "", result.KeyFile, "The default value for gcp-key-path should be an empty string")
	assert.Equal(t, "", result.BucketName, "The default value for gcp-bucket-name should be an empty string")
	assert.Equal(t, "", result.Endpoint, "The default value for gcp-endpoint-override should be an empty string")
	assert.Equal(t, models.DefaultChunkSize, result.ChunkSize, "The default value for gcp-chunk-size should be 5MB")
	assert.Equal(t, cloudMaxRetries, result.RetryMaxAttempts, "The default value for gcp-retry-max-attempts should be 100")
	assert.Equal(t, cloudMaxBackoff, result.RetryBackoffMaxSeconds, "The default value for gcp-retry-max-backoff should be 90")
	assert.Equal(t, cloudBackoff, result.RetryBackoffInitSeconds, "The default value for gcp-retry-init-backoff should be 60")
	assert.Equal(t, float64(2), result.RetryBackoffMultiplier, "The default value for gcp-retry-backoff-multiplier should be 2")
}
