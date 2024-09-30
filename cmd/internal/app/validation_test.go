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

	"github.com/aerospike/aerospike-client-go/v7"
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
		{
			name: "Partial AWS S3 configuration",
			awsS3: &models.AwsS3{
				Region:  "",
				Profile: "default",
			},
			gcpStorage: &models.GcpStorage{},
			azureBlob:  &models.AzureBlob{},
			wantErr:    false,
		},
		{
			name:  "Partial GCP Storage configuration",
			awsS3: &models.AwsS3{},
			gcpStorage: &models.GcpStorage{
				BucketName: "partial-bucket",
				KeyFile:    "",
			},
			azureBlob: &models.AzureBlob{},
			wantErr:   false,
		},
		{
			name:       "Partial Azure Blob configuration",
			awsS3:      &models.AwsS3{},
			gcpStorage: &models.GcpStorage{},
			azureBlob: &models.AzureBlob{
				ContainerName: "",
				AccountName:   "account-name",
				AccountKey:    "",
			},
			wantErr: false,
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

func TestValidateBackupParams(t *testing.T) {
	tests := []struct {
		name         string
		backupParams *models.Backup
		commonParams *models.Common
		wantErr      bool
		expectedErr  string
	}{
		{
			name: "Both AfterDigest and PartitionList configured",
			backupParams: &models.Backup{
				AfterDigest:   "some-digest",
				PartitionList: "some-partition",
			},
			commonParams: &models.Common{},
			wantErr:      true,
			expectedErr:  "only one of after-digest or partition-list can be configured",
		},
		{
			name: "Only AfterDigest configured",
			backupParams: &models.Backup{
				AfterDigest:   "some-digest",
				PartitionList: "",
				OutputFile:    "some-output-file",
			},
			commonParams: &models.Common{},
			wantErr:      false,
			expectedErr:  "",
		},
		{
			name: "Only PartitionList configured",
			backupParams: &models.Backup{
				AfterDigest:   "",
				PartitionList: "some-partition",
				OutputFile:    "some-output-file",
			},
			commonParams: &models.Common{},
			wantErr:      false,
			expectedErr:  "",
		},
		{
			name: "Neither AfterDigest nor PartitionList configured",
			backupParams: &models.Backup{
				AfterDigest:   "",
				PartitionList: "",
				OutputFile:    "some-output-file",
			},
			commonParams: &models.Common{},
			wantErr:      false,
			expectedErr:  "",
		},
		{
			name: "Estimate with PartitionList",
			backupParams: &models.Backup{
				Estimate:      true,
				PartitionList: "some-partition",
			},
			commonParams: &models.Common{},
			wantErr:      true,
			expectedErr:  "estimate with any filter is not allowed",
		},
		{
			name: "Estimate with output file",
			backupParams: &models.Backup{
				Estimate:   true,
				OutputFile: "output-file",
			},
			commonParams: &models.Common{},
			wantErr:      true,
			expectedErr:  "estimate with output-file or directory is not allowed",
		},
		{
			name: "Estimate with valid configuration",
			backupParams: &models.Backup{
				Estimate:        true,
				EstimateSamples: 100,
			},
			commonParams: &models.Common{},
			wantErr:      false,
			expectedErr:  "",
		},
		{
			name: "Estimate with invalid samples size",
			backupParams: &models.Backup{
				Estimate:        true,
				EstimateSamples: -1,
			},
			commonParams: &models.Common{},
			wantErr:      true,
			expectedErr:  "estimate with estimate-samples < 0 is not allowed",
		},
		{
			name: "Non-estimate with no output or directory",
			backupParams: &models.Backup{
				Estimate:   false,
				OutputFile: "",
			},
			commonParams: &models.Common{
				Directory: "",
			},
			wantErr:     true,
			expectedErr: "must specify either output-file or directory",
		},
		{
			name: "Non-estimate with output file",
			backupParams: &models.Backup{
				Estimate:   false,
				OutputFile: "output-file",
			},
			commonParams: &models.Common{
				Directory: "",
			},
			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Non-estimate with directory",
			backupParams: &models.Backup{
				Estimate:   false,
				OutputFile: "",
			},
			commonParams: &models.Common{
				Directory: "some-directory",
			},
			wantErr:     false,
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBackupParams(tt.backupParams, tt.commonParams)
			if tt.wantErr {
				assert.Error(t, err, "Expected error but got none")
				assert.Equal(t, tt.expectedErr, err.Error())
			} else {
				assert.NoError(t, err, "Expected no error but got one")
			}
		})
	}
}

func TestValidatePartitionFilters(t *testing.T) {
	tests := []struct {
		name             string
		partitionFilters []*aerospike.PartitionFilter
		wantErr          bool
		expectedErr      string
	}{
		{
			name: "Single valid partition filter",
			partitionFilters: []*aerospike.PartitionFilter{
				{Begin: 0, Count: 1},
			},
			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Non-overlapping partition filters",
			partitionFilters: []*aerospike.PartitionFilter{
				{Begin: 0, Count: 5},
				{Begin: 10, Count: 5},
			},
			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Overlapping partition filters",
			partitionFilters: []*aerospike.PartitionFilter{
				{Begin: 0, Count: 10},
				{Begin: 5, Count: 10},
			},
			wantErr:     true,
			expectedErr: "overlapping intervals: [0, 10] and [5, 15]",
		},
		{
			name: "Duplicate begin value",
			partitionFilters: []*aerospike.PartitionFilter{
				{Begin: 0, Count: 1},
				{Begin: 0, Count: 1},
			},
			wantErr:     true,
			expectedErr: "duplicate begin value 0 for count = 1",
		},
		{
			name: "Mixed filters with no overlap",
			partitionFilters: []*aerospike.PartitionFilter{
				{Begin: 0, Count: 1},
				{Begin: 5, Count: 5},
				{Begin: 20, Count: 1},
				{Begin: 30, Count: 10},
			},
			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Invalid count in filter",
			partitionFilters: []*aerospike.PartitionFilter{
				{Begin: 0, Count: 0},
			},
			wantErr:     true,
			expectedErr: "invalid partition filter count: 0",
		},
		{
			name:             "Edge case: Empty filters",
			partitionFilters: []*aerospike.PartitionFilter{},
			wantErr:          false,
			expectedErr:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePartitionFilters(tt.partitionFilters)
			if tt.wantErr {
				assert.Error(t, err, "Expected error but got none")
				assert.Equal(t, tt.expectedErr, err.Error())
			} else {
				assert.NoError(t, err, "Expected no error but got one")
			}
		})
	}
}
