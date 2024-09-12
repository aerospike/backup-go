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

func TestAzureBlob_NewFlagSet(t *testing.T) {
	t.Parallel()
	azureBlob := NewAzureBlob()

	flagSet := azureBlob.NewFlagSet()

	args := []string{
		"--azure-account-name", "myaccount",
		"--azure-account-key", "mykey",
		"--azure-tenant-id", "tenant-id",
		"--azure-client-id", "client-id",
		"--azure-client-secret", "client-secret",
		"--azure-endpoint", "https://custom-endpoint.com",
		"--azure-container-name", "my-container",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	result := azureBlob.GetAzureBlob()

	assert.Equal(t, "myaccount", result.AccountName, "The azure-account-name flag should be parsed correctly")
	assert.Equal(t, "mykey", result.AccountKey, "The azure-account-key flag should be parsed correctly")
	assert.Equal(t, "tenant-id", result.TenantID, "The azure-tenant-id flag should be parsed correctly")
	assert.Equal(t, "client-id", result.ClientID, "The azure-client-id flag should be parsed correctly")
	assert.Equal(t, "client-secret", result.ClientSecret, "The azure-client-secret flag should be parsed correctly")
	assert.Equal(t, "https://custom-endpoint.com", result.Endpoint, "The azure-endpoint flag should be parsed correctly")
	assert.Equal(t, "my-container", result.ContainerName, "The azure-container-name flag should be parsed correctly")
}

func TestAzureBlob_NewFlagSet_DefaultValues(t *testing.T) {
	t.Parallel()
	azureBlob := NewAzureBlob()

	flagSet := azureBlob.NewFlagSet()

	err := flagSet.Parse([]string{})
	assert.NoError(t, err)

	result := azureBlob.GetAzureBlob()

	assert.Equal(t, "", result.AccountName, "The default value for azure-account-name should be an empty string")
	assert.Equal(t, "", result.AccountKey, "The default value for azure-account-key should be an empty string")
	assert.Equal(t, "", result.TenantID, "The default value for azure-tenant-id should be an empty string")
	assert.Equal(t, "", result.ClientID, "The default value for azure-client-id should be an empty string")
	assert.Equal(t, "", result.ClientSecret, "The default value for azure-client-secret should be an empty string")
	assert.Equal(t, "", result.Endpoint, "The default value for azure-endpoint should be an empty string")
	assert.Equal(t, "", result.ContainerName, "The default value for azure-container-name should be an empty string")
}
