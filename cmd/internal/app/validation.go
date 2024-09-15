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
	"fmt"

	"github.com/aerospike/backup-go/cmd/internal/models"
)

func validateStorages(
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
) error {
	var count int

	if awsS3.Region != "" || awsS3.Profile != "" || awsS3.Endpoint != "" {
		count++
	}

	if gcpStorage.BucketName != "" || gcpStorage.KeyFile != "" || gcpStorage.Endpoint != "" {
		count++
	}

	if azureBlob.ContainerName != "" || azureBlob.AccountName != "" || azureBlob.AccountKey != "" ||
		azureBlob.Endpoint != "" || azureBlob.TenantID != "" || azureBlob.ClientID != "" ||
		azureBlob.ClientSecret != "" {
		count++
	}

	if count > 1 {
		return fmt.Errorf("only one cloud provider can be configured")
	}

	return nil
}
