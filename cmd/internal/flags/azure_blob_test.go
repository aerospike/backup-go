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

func TestAzureBlob_NewFlagSetRestore(t *testing.T) {
	t.Parallel()
	azureBlob := NewAzureBlob(OperationRestore)

	flagSet := azureBlob.NewFlagSet()

	args := []string{
		"--azure-account-name", "myaccount",
		"--azure-account-key", "mykey",
		"--azure-tenant-id", "tenant-id",
		"--azure-client-id", "client-id",
		"--azure-client-secret", "client-secret",
		"--azure-endpoint", "https://custom-endpoint.com",
		"--azure-container-name", "my-container",
		"--azure-access-tier", "Standard",
		"--azure-rehydrate-poll-duration", "1000",
		"--azure-retry-max-attempts", "10",
		"--azure-retry-max-delay", "10",
		"--azure-retry-delay", "10",
		"--azure-retry-timeout", "10",
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
	assert.Equal(t, "Standard", result.AccessTier, "The azure-access-tier flag should be parsed correctly")
	assert.Equal(t, int64(1000), result.RestorePollDuration, "The azure-rehydrate-poll-duration flag should be parsed correctly")
	assert.Equal(t, 10, result.RetryMaxAttempts, "The azure-retry-max-attempts flag should be parsed correctly")
	assert.Equal(t, 10, result.RetryMaxDelaySeconds, "The azure-retry-max-delay flag should be parsed correctly")
	assert.Equal(t, 10, result.RetryDelaySeconds, "The azure-retry-delay flag should be parsed correctly")
	assert.Equal(t, 10, result.RetryTryTimeoutSeconds, "The azure-retry-timeout flag should be parsed correctly")
}

func TestAzureBlob_NewFlagSet_DefaultValuesRestore(t *testing.T) {
	t.Parallel()
	azureBlob := NewAzureBlob(OperationRestore)

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
	assert.Equal(t, "", result.AccessTier, "The default value for azure-access-tier should be an empty string")
	assert.Equal(t, int64(60000), result.RestorePollDuration, "The default value for azure-rehydrate-poll-duration should be 60000")
	assert.Equal(t, 100, result.RetryMaxAttempts, "The default value for azure-retry-max-attempts flag should be 100")
	assert.Equal(t, 90, result.RetryMaxDelaySeconds, "The default value for azure-retry-max-delay flag should be 90")
	assert.Equal(t, 60, result.RetryDelaySeconds, "The default value for azure-retry-delay flag should be 60")
	assert.Equal(t, 0, result.RetryTryTimeoutSeconds, "The default value for azure-retry-timeout flag should be 0")
}

func TestAzureBlob_NewFlagSetBackup(t *testing.T) {
	t.Parallel()
	azureBlob := NewAzureBlob(OperationBackup)

	flagSet := azureBlob.NewFlagSet()

	args := []string{
		"--azure-account-name", "myaccount",
		"--azure-account-key", "mykey",
		"--azure-tenant-id", "tenant-id",
		"--azure-client-id", "client-id",
		"--azure-client-secret", "client-secret",
		"--azure-endpoint", "https://custom-endpoint.com",
		"--azure-container-name", "my-container",
		"--azure-access-tier", "Standard",
		"--azure-block-size", "1",
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
	assert.Equal(t, "Standard", result.AccessTier, "The azure-access-tier flag should be parsed correctly")
	assert.Equal(t, 1, result.BlockSize, "The azure-block-size flag should be parsed correctly")
}

func TestAzureBlob_NewFlagSet_DefaultValuesBackup(t *testing.T) {
	t.Parallel()
	azureBlob := NewAzureBlob(OperationBackup)

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
	assert.Equal(t, "", result.AccessTier, "The default value for azure-access-tier should be an empty string")
	assert.Equal(t, models.DefaultChunkSize, result.BlockSize, "The default value for azure-block-size should be 5MB")
}
