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
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/spf13/pflag"
)

const (
	descAccessTierBackup  = "Azure access tier is applied to created backup files."
	descAccessTierRestore = "If is set, tool will try to rehydrate archived files to the specified tier."
)

type AzureBlob struct {
	operation int
	models.AzureBlob
}

func NewAzureBlob(operation int) *AzureBlob {
	return &AzureBlob{
		operation: operation,
	}
}

func (f *AzureBlob) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	var descAccessTier string

	switch f.operation {
	case OperationBackup:
		descAccessTier = descAccessTierBackup
	case OperationRestore:
		descAccessTier = descAccessTierRestore
	}

	flagSet.StringVar(&f.AccountName, "azure-account-name",
		"",
		"Azure account name for account name, key authorization.")
	flagSet.StringVar(&f.AccountKey, "azure-account-key",
		"",
		"Azure account key for account name, key authorization.")
	flagSet.StringVar(&f.TenantID, "azure-tenant-id",
		"",
		"Azure tenant ID for Azure Active Directory authorization.")
	flagSet.StringVar(&f.ClientID, "azure-client-id",
		"",
		"Azure client ID for Azure Active Directory authorization.")
	flagSet.StringVar(&f.ClientSecret, "azure-client-secret",
		"",
		"Azure client secret for Azure Active Directory authorization.")
	flagSet.StringVar(&f.Endpoint, "azure-endpoint",
		"",
		"Azure endpoint.")
	flagSet.StringVar(&f.ContainerName, "azure-container-name",
		"",
		"Azure container Name.")
	flagSet.StringVar(&f.AccessTier, "azure-access-tier",
		"",
		descAccessTier+
			"\nTiers are: Archive, Cold, Cool, Hot, P10, P15, P20, P30, P4, P40, P50, P6, P60, P70, P80, Premium.")

	switch f.operation {
	case OperationBackup:
		flagSet.IntVar(&f.BlockSize, "azure-block-size",
			models.DefaultChunkSize,
			"Block size defines the size of the buffer used during upload.")
	case OperationRestore:
		flagSet.Int64Var(&f.RestorePollDuration, "azure-rehydrate-poll-duration",
			60000,
			"How often (in milliseconds) a backup client checks object status when restoring an archived object.")
	}

	flagSet.IntVar(&f.RetryMaxAttempts, "azure-retry-max-attempts",
		cloudMaxRetries,
		"Max retries specifies the maximum number of attempts a failed operation will be retried\n"+
			"before producing an error.")
	flagSet.IntVar(&f.RetryMaxDelaySeconds, "azure-retry-max-delay",
		cloudMaxBackoff,
		"Max retry delay specifies the maximum delay in seconds allowed before retrying an operation.\n"+
			"Typically the value is greater than or equal to the value specified in azure-retry-delay.")
	flagSet.IntVar(&f.RetryDelaySeconds, "azure-retry-delay",
		cloudBackoff,
		"Retry delay specifies the initial amount of delay in seconds to use before retrying an operation.\n"+
			"The value is used only if the HTTP response does not contain a Retry-After header.\n"+
			"The delay increases exponentially with each retry up to the maximum specified by azure-retry-max-delay.")
	flagSet.IntVar(&f.RetryTimeoutSeconds, "azure-retry-timeout",
		0,
		"Retry timeout in seconds indicates the maximum time allowed for any single try of an HTTP request.\n"+
			"This is disabled by default. Specify a value greater than zero to enable.\n"+
			"NOTE: Setting this to a small value might cause premature HTTP request time-outs.")

	return flagSet
}

func (f *AzureBlob) GetAzureBlob() *models.AzureBlob {
	return &f.AzureBlob
}
