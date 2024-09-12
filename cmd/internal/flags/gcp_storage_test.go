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

	"github.com/stretchr/testify/assert"
)

func TestGcpStorage_NewFlagSet(t *testing.T) {
	t.Parallel()
	gcpStorage := NewGcpStorage()

	flagSet := gcpStorage.NewFlagSet()

	args := []string{
		"--gcp-key-path", "/path/to/keyfile.json",
		"--gcp-bucket-name", "my-bucket",
		"--gcp-endpoint-override", "https://gcp.custom-endpoint.com",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	result := gcpStorage.GetGcpStorage()

	assert.Equal(t, "/path/to/keyfile.json", result.KeyFile, "The gcp-key-path flag should be parsed correctly")
	assert.Equal(t, "my-bucket", result.BucketName, "The gcp-bucket-name flag should be parsed correctly")
	assert.Equal(t, "https://gcp.custom-endpoint.com", result.Endpoint, "The gcp-endpoint-override flag should be parsed correctly")
}

func TestGcpStorage_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()
	gcpStorage := NewGcpStorage()

	flagSet := gcpStorage.NewFlagSet()

	err := flagSet.Parse([]string{})
	assert.NoError(t, err)

	result := gcpStorage.GetGcpStorage()

	assert.Equal(t, "", result.KeyFile, "The default value for gcp-key-path should be an empty string")
	assert.Equal(t, "", result.BucketName, "The default value for gcp-bucket-name should be an empty string")
	assert.Equal(t, "", result.Endpoint, "The default value for gcp-endpoint-override should be an empty string")
}
