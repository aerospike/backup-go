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

	cmn := &models.Common{}

	err := validateBackupParams(cfg, cmn)
	assert.Error(t, err)
	assert.Equal(t, "only one of after-digest or partition-list can be configured", err.Error())

	cfg = &models.Backup{
		AfterDigest:   "some-digest",
		PartitionList: "",
	}
	err = validateBackupParams(cfg, cmn)
	assert.NoError(t, err)

	cfg = &models.Backup{
		AfterDigest:   "",
		PartitionList: "some-partition",
	}
	err = validateBackupParams(cfg, cmn)
	assert.NoError(t, err)

	cfg = &models.Backup{
		AfterDigest:   "",
		PartitionList: "",
	}
	err = validateBackupParams(cfg, cmn)
	assert.NoError(t, err)
}

func TestValidatePartitionFilters_SuccessSingleFilter(t *testing.T) {
	t.Parallel()

	partitionFilters := []*aerospike.PartitionFilter{
		{Begin: 0, Count: 1},
	}

	err := validatePartitionFilters(partitionFilters)

	assert.NoError(t, err, "Single partition filter should be valid")
}

func TestValidatePartitionFilters_SuccessNonOverlappingIntervals(t *testing.T) {
	t.Parallel()

	partitionFilters := []*aerospike.PartitionFilter{
		{Begin: 0, Count: 5},
		{Begin: 10, Count: 5},
		{Begin: 20, Count: 10},
	}

	err := validatePartitionFilters(partitionFilters)

	assert.NoError(t, err, "Non-overlapping intervals should be valid")
}

func TestValidatePartitionFilters_ErrorDuplicateBeginValue(t *testing.T) {
	t.Parallel()

	partitionFilters := []*aerospike.PartitionFilter{
		{Begin: 0, Count: 1},
		{Begin: 0, Count: 1},
	}

	err := validatePartitionFilters(partitionFilters)

	assert.Error(t, err, "Duplicate begin value should return an error")
	assert.Equal(t, "duplicate begin value 0 for count = 1", err.Error())
}

func TestValidatePartitionFilters_ErrorOverlappingIntervals(t *testing.T) {
	t.Parallel()

	partitionFilters := []*aerospike.PartitionFilter{
		{Begin: 0, Count: 10},
		{Begin: 5, Count: 10},
	}

	err := validatePartitionFilters(partitionFilters)

	assert.Error(t, err, "Overlapping intervals should return an error")
	assert.Equal(t, "overlapping intervals: [0, 10] and [5, 15]", err.Error())
}

func TestValidatePartitionFilters_SuccessMixedFilters(t *testing.T) {
	t.Parallel()

	partitionFilters := []*aerospike.PartitionFilter{
		{Begin: 0, Count: 1},
		{Begin: 5, Count: 5},
		{Begin: 20, Count: 1},
		{Begin: 30, Count: 10},
	}

	err := validatePartitionFilters(partitionFilters)

	assert.NoError(t, err, "Valid mixed filters should pass validation")
}

func TestValidatePartitionFilters_ErrorInvalidCount(t *testing.T) {
	t.Parallel()

	partitionFilters := []*aerospike.PartitionFilter{
		{Begin: 0, Count: 0},
	}

	err := validatePartitionFilters(partitionFilters)

	assert.Error(t, err, "Invalid count should return an error")
	assert.Equal(t, "invalid partition filter count: 0", err.Error())
}

func TestValidatePartitionFilters_ErrorOverlappingComplexIntervals(t *testing.T) {
	t.Parallel()

	partitionFilters := []*aerospike.PartitionFilter{
		{Begin: 0, Count: 5},
		{Begin: 5, Count: 10},
		{Begin: 8, Count: 3},
	}

	err := validatePartitionFilters(partitionFilters)

	assert.Error(t, err, "Overlapping complex intervals should return an error")
	assert.Equal(t, "overlapping intervals: [0, 5] and [5, 15]", err.Error())
}
