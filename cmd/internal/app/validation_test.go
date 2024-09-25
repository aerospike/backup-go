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
	"testing"

	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/stretchr/testify/assert"
)

func TestValidateStorages(t *testing.T) {
	tests := []struct {
		name       string
		awsS3      *models.AwsS3
		gcpStorage *models.GcpStorage
		azureBlob  *models.AzureBlob
		wantErr    bool
	}{
		{
			name: "Valid AWS S3 configuration only",
			awsS3: &models.AwsS3{
				Region: "us-west-2",
			},
			gcpStorage: &models.GcpStorage{},
			azureBlob:  &models.AzureBlob{},
			wantErr:    false,
		},
		{
			name:  "Valid GCP Storage configuration only",
			awsS3: &models.AwsS3{},
			gcpStorage: &models.GcpStorage{
				BucketName: "my-bucket",
			},
			azureBlob: &models.AzureBlob{},
			wantErr:   false,
		},
		{
			name:       "Valid Azure Blob configuration only",
			awsS3:      &models.AwsS3{},
			gcpStorage: &models.GcpStorage{},
			azureBlob: &models.AzureBlob{
				ContainerName: "my-container",
				AccountName:   "account-name",
				AccountKey:    "account-key",
			},
			wantErr: false,
		},
		{
			name: "AWS S3 and GCP Storage both configured",
			awsS3: &models.AwsS3{
				Region: "us-west-2",
			},
			gcpStorage: &models.GcpStorage{
				BucketName: "my-bucket",
			},
			azureBlob: &models.AzureBlob{},
			wantErr:   true,
		},
		{
			name: "All three providers configured",
			awsS3: &models.AwsS3{
				Region: "us-west-2",
			},
			gcpStorage: &models.GcpStorage{
				BucketName: "my-bucket",
			},
			azureBlob: &models.AzureBlob{
				ContainerName: "my-container",
				AccountName:   "account-name",
			},
			wantErr: true,
		},
		{
			name:       "None of the providers configured",
			awsS3:      &models.AwsS3{},
			gcpStorage: &models.GcpStorage{},
			azureBlob:  &models.AzureBlob{},
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateStorages(tt.awsS3, tt.gcpStorage, tt.azureBlob)
			if tt.wantErr {
				assert.Error(t, err, "Expected error but got none")
			} else {
				assert.NoError(t, err, "Expected no error but got one")
			}
		})
	}
}

func TestValidateBackupConfig(t *testing.T) {
	t.Parallel()

	cfg := &models.Backup{
		AfterDigest:   "some-digest",
		PartitionList: "some-partition",
	}
	err := validateBackupParams(cfg)
	assert.Error(t, err)
	assert.Equal(t, "only one of after-digest or partition-list can be configured", err.Error())

	cfg = &models.Backup{
		AfterDigest:   "some-digest",
		PartitionList: "",
	}
	err = validateBackupParams(cfg)
	assert.NoError(t, err)

	cfg = &models.Backup{
		AfterDigest:   "",
		PartitionList: "some-partition",
	}
	err = validateBackupParams(cfg)
	assert.NoError(t, err)

	cfg = &models.Backup{
		AfterDigest:   "",
		PartitionList: "",
	}
	err = validateBackupParams(cfg)
	assert.NoError(t, err)
}
