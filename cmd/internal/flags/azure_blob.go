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

type AzureBlob struct {
	models.AzureBlob
}

func NewAzureBlob() *AzureBlob {
	return &AzureBlob{}
}

func (f *AzureBlob) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

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
	flagSet.StringVar(&f.RestoreTier, "azure-restore-tier",
		"",
		"tier")

	return flagSet
}

func (f *AzureBlob) GetAzureBlob() *models.AzureBlob {
	return &f.AzureBlob
}
