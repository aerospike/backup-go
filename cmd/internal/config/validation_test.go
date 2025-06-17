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

package config

import (
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/stretchr/testify/assert"
)

const (
	testBucket = "test-bucket"
)

func TestValidateStorages(t *testing.T) {
	t.Parallel()

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
				Region:     "us-west-2",
				BucketName: testBucket,
			},
			gcpStorage: &models.GcpStorage{},
			azureBlob:  &models.AzureBlob{},
			wantErr:    false,
		},
		{
			name:  "Valid GCP Storage configuration only",
			awsS3: &models.AwsS3{},
			gcpStorage: &models.GcpStorage{
				BucketName:             testBucket,
				RetryBackoffMultiplier: 2,
			},
			azureBlob: &models.AzureBlob{},
			wantErr:   false,
		},
		{
			name:       "Valid Azure Blob configuration only",
			awsS3:      &models.AwsS3{},
			gcpStorage: &models.GcpStorage{},
			azureBlob: &models.AzureBlob{
				ContainerName: testBucket,
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
		{
			name: "Partial AWS S3 configuration",
			awsS3: &models.AwsS3{
				BucketName: testBucket,
				Region:     "",
				Profile:    "default",
			},
			gcpStorage: &models.GcpStorage{},
			azureBlob:  &models.AzureBlob{},
			wantErr:    false,
		},
		{
			name:  "Partial GCP Storage configuration",
			awsS3: &models.AwsS3{},
			gcpStorage: &models.GcpStorage{
				BucketName:             testBucket,
				KeyFile:                "",
				RetryBackoffMultiplier: 2,
			},
			azureBlob: &models.AzureBlob{},
			wantErr:   false,
		},
		{
			name:       "Partial Azure Blob configuration",
			awsS3:      &models.AwsS3{},
			gcpStorage: &models.GcpStorage{},
			azureBlob: &models.AzureBlob{
				ContainerName: testBucket,
				AccountName:   "account-name",
				AccountKey:    "",
			},
			wantErr: false,
		},
		{
			name:       "Azure Blob with client credentials",
			awsS3:      &models.AwsS3{},
			gcpStorage: &models.GcpStorage{},
			azureBlob: &models.AzureBlob{
				ContainerName: testBucket,
				TenantID:      "tenant-id",
				ClientID:      "client-id",
				ClientSecret:  "client-secret",
			},
			wantErr: false,
		},
		{
			name: "Only endpoints configured",
			awsS3: &models.AwsS3{
				BucketName: "custom-bucket",
				Endpoint:   "custom-endpoint",
			},
			gcpStorage: &models.GcpStorage{},
			azureBlob:  &models.AzureBlob{},
			wantErr:    false,
		},
		{
			name: "Multiple providers with only endpoints",
			awsS3: &models.AwsS3{
				Endpoint: "aws-endpoint",
			},
			gcpStorage: &models.GcpStorage{
				Endpoint: "gcp-endpoint",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateStorages(tt.awsS3, tt.gcpStorage, tt.azureBlob)
			if tt.wantErr {
				assert.Error(t, err, "Expected error but got none")
			} else {
				assert.NoError(t, err, "Expected no error but got one")
			}
		})
	}
}

func TestValidatePartitionFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		partitionFilters []*aerospike.PartitionFilter
		wantErr          bool
	}{
		{
			name: "Single valid partition filter",
			partitionFilters: []*aerospike.PartitionFilter{
				{Begin: 0, Count: 1},
			},
			wantErr: false,
		},
		{
			name: "Non-overlapping partition filters",
			partitionFilters: []*aerospike.PartitionFilter{
				{Begin: 0, Count: 5},
				{Begin: 10, Count: 5},
			},
			wantErr: false,
		},
		{
			name: "Overlapping partition filters",
			partitionFilters: []*aerospike.PartitionFilter{
				{Begin: 0, Count: 10},
				{Begin: 5, Count: 10},
			},
			wantErr: true,
		},
		{
			name: "Duplicate begin value",
			partitionFilters: []*aerospike.PartitionFilter{
				{Begin: 0, Count: 1},
				{Begin: 0, Count: 1},
			},
			wantErr: true,
		},
		{
			name: "Mixed filters with no overlap",
			partitionFilters: []*aerospike.PartitionFilter{
				{Begin: 0, Count: 1},
				{Begin: 5, Count: 5},
				{Begin: 20, Count: 1},
				{Begin: 30, Count: 10},
			},
			wantErr: false,
		},
		{
			name: "Invalid count in filter",
			partitionFilters: []*aerospike.PartitionFilter{
				{Begin: 0, Count: 0},
			},
			wantErr: true,
		},
		{
			name:             "Edge case: Empty filters",
			partitionFilters: []*aerospike.PartitionFilter{},
			wantErr:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidatePartitionFilters(tt.partitionFilters)
			if tt.wantErr {
				assert.Error(t, err, "Expected error but got none")
			} else {
				assert.NoError(t, err, "Expected no error but got one")
			}
		})
	}
}
