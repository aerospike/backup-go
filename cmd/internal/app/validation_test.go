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

	"github.com/aerospike/aerospike-client-go/v8"
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
		{
			name:       "Azure Blob with client credentials",
			awsS3:      &models.AwsS3{},
			gcpStorage: &models.GcpStorage{},
			azureBlob: &models.AzureBlob{
				TenantID:     "tenant-id",
				ClientID:     "client-id",
				ClientSecret: "client-secret",
			},
			wantErr: false,
		},
		{
			name: "Only endpoints configured",
			awsS3: &models.AwsS3{
				Endpoint: "custom-endpoint",
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
		{
			name: "Continue and nodes",
			backupParams: &models.Backup{
				StateFileDst:  "some-file",
				ParallelNodes: true,
				OutputFile:    "some-output-file",
			},
			commonParams: &models.Common{},
			wantErr:      true,
			expectedErr:  "saving states and calculating estimates is not possible in parallel node mode",
		},
		{
			name: "Continue with valid state file",
			backupParams: &models.Backup{
				Continue:   "state.json",
				OutputFile: "output.asb",
			},
			commonParams: &models.Common{},
			wantErr:      false,
		},
		{
			name: "NodeList with parallel nodes",
			backupParams: &models.Backup{
				NodeList:      "node1,node2",
				ParallelNodes: true,
				OutputFile:    "output.asb",
			},
			commonParams: &models.Common{},
			wantErr:      false,
		},
		{
			name: "FilterExpression with valid expression",
			backupParams: &models.Backup{
				FilterExpression: "age > 25",
				OutputFile:       "output.asb",
			},
			commonParams: &models.Common{},
			wantErr:      false,
		},
		{
			name: "Modified time filters",
			backupParams: &models.Backup{
				ModifiedAfter:  "2024-01-01",
				ModifiedBefore: "2024-12-31",
				OutputFile:     "output.asb",
			},
			commonParams: &models.Common{},
			wantErr:      false,
		},
		{
			name: "NoTTLOnly flag",
			backupParams: &models.Backup{
				NoTTLOnly:  true,
				OutputFile: "output.asb",
			},
			commonParams: &models.Common{},
			wantErr:      false,
		},
		{
			name: "Estimate with FilterExpression",
			backupParams: &models.Backup{
				Estimate:         true,
				FilterExpression: "age > 25",
			},
			commonParams: &models.Common{},
			wantErr:      true,
			expectedErr:  "estimate with any filter is not allowed",
		},
		{
			name: "Estimate with ModifiedAfter",
			backupParams: &models.Backup{
				Estimate:      true,
				ModifiedAfter: "2024-01-01",
			},
			commonParams: &models.Common{},
			wantErr:      true,
			expectedErr:  "estimate with any filter is not allowed",
		},
		{
			name: "Both directory and output file configured",
			backupParams: &models.Backup{
				OutputFile: "output.asb",
			},
			commonParams: &models.Common{
				Directory: "backup-dir",
			},
			wantErr:     true,
			expectedErr: "only one of output-file and directory may be configured at the same time",
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
			err := validatePartitionFilters(tt.partitionFilters)
			if tt.wantErr {
				assert.Error(t, err, "Expected error but got none")
			} else {
				assert.NoError(t, err, "Expected no error but got one")
			}
		})
	}
}

func TestValidateCommonParams(t *testing.T) {
	tests := []struct {
		name         string
		commonParams *models.Common
		wantErr      bool
		expectedErr  string
	}{
		{
			name: "Valid total and socket timeout",
			commonParams: &models.Common{
				TotalTimeout:  1000,
				SocketTimeout: 500,
				Namespace:     testNamespace,
			},
			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Invalid negative total timeout",
			commonParams: &models.Common{
				TotalTimeout:  -1,
				SocketTimeout: 500,
				Namespace:     testNamespace,
			},
			wantErr:     true,
			expectedErr: "total-timeout must be non-negative",
		},
		{
			name: "Invalid negative socket timeout",
			commonParams: &models.Common{
				TotalTimeout:  1000,
				SocketTimeout: -1,
				Namespace:     testNamespace,
			},
			wantErr:     true,
			expectedErr: "socket-timeout must be non-negative",
		},
		{
			name: "Both total and socket timeout negative",
			commonParams: &models.Common{
				TotalTimeout:  -1000,
				SocketTimeout: -500,
				Namespace:     testNamespace,
			},
			wantErr:     true,
			expectedErr: "total-timeout must be non-negative",
		},
		{
			name: "Edge case: zero total and socket timeout",
			commonParams: &models.Common{
				TotalTimeout:  0,
				SocketTimeout: 0,
				Namespace:     testNamespace,
			},
			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "Missing namespace",
			commonParams: &models.Common{
				TotalTimeout:  1000,
				SocketTimeout: 500,
				Namespace:     "",
			},
			wantErr:     true,
			expectedErr: "namespace is required",
		},
		{
			name: "Valid namespace with all timeouts",
			commonParams: &models.Common{
				TotalTimeout:  1000,
				SocketTimeout: 500,
				Namespace:     "test-ns",
			},
			wantErr:     false,
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCommonParams(tt.commonParams)
			if tt.wantErr {
				assert.Error(t, err, "Expected error but got none")
				assert.Equal(t, tt.expectedErr, err.Error())
			} else {
				assert.NoError(t, err, "Expected no error but got one")
			}
		})
	}
}

func TestValidateRestoreParams(t *testing.T) {
	tests := []struct {
		name          string
		restoreParams *models.Restore
		commonParams  *models.Common
		expectedError string
	}{
		{
			name: "Valid input - only Directory set",
			restoreParams: &models.Restore{
				InputFile:       "",
				DirectoryList:   "",
				ParentDirectory: "",
			},
			commonParams: &models.Common{
				Directory: "some/directory",
			},
			expectedError: "",
		},
		{
			name: "Error - both Directory and InputFile set",
			restoreParams: &models.Restore{
				InputFile:       "somefile",
				DirectoryList:   "",
				ParentDirectory: "",
			},
			commonParams: &models.Common{
				Directory: "some/directory",
			},
			expectedError: "only one of directory and input-file may be configured at the same time",
		},
		{
			name: "Error - DirectoryList with Directory",
			restoreParams: &models.Restore{
				InputFile:       "",
				DirectoryList:   "dirlist",
				ParentDirectory: "",
			},
			commonParams: &models.Common{
				Directory: "some/directory",
			},
			expectedError: "only one of directory, input-file and directory-list may be configured at the same time",
		},
		{
			name: "Error - DirectoryList with InputFile",
			restoreParams: &models.Restore{
				InputFile:       "somefile",
				DirectoryList:   "dirlist",
				ParentDirectory: "",
			},
			commonParams: &models.Common{
				Directory: "",
			},
			expectedError: "only one of directory, input-file and directory-list may be configured at the same time",
		},
		{
			name: "Error - ParentDirectory without DirectoryList",
			restoreParams: &models.Restore{
				InputFile:       "",
				DirectoryList:   "",
				ParentDirectory: "parent/dir",
			},
			commonParams: &models.Common{
				Directory: "",
			},
			expectedError: "must specify directory-list list",
		},
		{
			name: "Valid input - DirectoryList with ParentDirectory",
			restoreParams: &models.Restore{
				InputFile:       "",
				DirectoryList:   "dirlist",
				ParentDirectory: "parent/dir",
			},
			commonParams: &models.Common{
				Directory: "",
			},
			expectedError: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRestoreParams(tt.restoreParams, tt.commonParams)
			if tt.expectedError == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.expectedError)
			}
		})
	}
}

func Test_validateBackupXDRParams(t *testing.T) {
	tests := []struct {
		name    string
		params  *models.BackupXDR
		wantErr string
	}{
		{
			name: "valid params",
			params: &models.BackupXDR{
				ReadTimeoutMilliseconds:      1000,
				WriteTimeoutMilliseconds:     1000,
				InfoPolingPeriodMilliseconds: 1000,
				StartTimeoutMilliseconds:     1000,
				ResultQueueSize:              100,
				AckQueueSize:                 100,
				MaxConnections:               10,
				ParallelWrite:                5,
				FileLimit:                    1000,
			},
			wantErr: "",
		},
		{
			name: "negative read timeout",
			params: &models.BackupXDR{
				ReadTimeoutMilliseconds: -1,
			},
			wantErr: "backup xdr read timeout can't be negative",
		},
		{
			name: "negative write timeout",
			params: &models.BackupXDR{
				ReadTimeoutMilliseconds:  0,
				WriteTimeoutMilliseconds: -1,
			},
			wantErr: "backup xdr write timeout can't be negative",
		},
		{
			name: "negative info polling period",
			params: &models.BackupXDR{
				ReadTimeoutMilliseconds:      0,
				WriteTimeoutMilliseconds:     0,
				InfoPolingPeriodMilliseconds: -1,
			},
			wantErr: "backup xdr info poling period can't be negative",
		},
		{
			name: "negative start timeout",
			params: &models.BackupXDR{
				ReadTimeoutMilliseconds:      0,
				WriteTimeoutMilliseconds:     0,
				InfoPolingPeriodMilliseconds: 0,
				StartTimeoutMilliseconds:     -1,
			},
			wantErr: "backup xdr start timeout can't be negative",
		},
		{
			name: "negative result queue size",
			params: &models.BackupXDR{
				ReadTimeoutMilliseconds:      0,
				WriteTimeoutMilliseconds:     0,
				InfoPolingPeriodMilliseconds: 0,
				StartTimeoutMilliseconds:     0,
				ResultQueueSize:              -1,
			},
			wantErr: "backup xdr result queue size can't be negative",
		},
		{
			name: "negative ack queue size",
			params: &models.BackupXDR{
				ReadTimeoutMilliseconds:      0,
				WriteTimeoutMilliseconds:     0,
				InfoPolingPeriodMilliseconds: 0,
				StartTimeoutMilliseconds:     0,
				ResultQueueSize:              0,
				AckQueueSize:                 -1,
			},
			wantErr: "backup xdr ack queue size can't be negative",
		},
		{
			name: "invalid max connections",
			params: &models.BackupXDR{
				ReadTimeoutMilliseconds:      0,
				WriteTimeoutMilliseconds:     0,
				InfoPolingPeriodMilliseconds: 0,
				StartTimeoutMilliseconds:     0,
				ResultQueueSize:              0,
				AckQueueSize:                 0,
				MaxConnections:               0,
			},
			wantErr: "backup xdr max connections can't be less than 1",
		},
		{
			name: "invalid file limit",
			params: &models.BackupXDR{
				ReadTimeoutMilliseconds:      0,
				WriteTimeoutMilliseconds:     0,
				InfoPolingPeriodMilliseconds: 0,
				StartTimeoutMilliseconds:     0,
				ResultQueueSize:              0,
				AckQueueSize:                 0,
				MaxConnections:               1,
				ParallelWrite:                1,
				FileLimit:                    0,
			},
			wantErr: "backup xdr file limit can't be less than 1",
		},
		{
			name: "negative info retry interval",
			params: &models.BackupXDR{
				MaxConnections:                1,
				ParallelWrite:                 1,
				FileLimit:                     1,
				InfoRetryIntervalMilliseconds: -1,
			},
			wantErr: "backup xdr info retry interval can't be negative",
		},
		{
			name: "negative info retries multiplier",
			params: &models.BackupXDR{
				MaxConnections:        1,
				ParallelWrite:         1,
				FileLimit:             1,
				InfoRetriesMultiplier: -1,
			},
			wantErr: "backup xdr info retries multiplier can't be negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBackupXDRParams(tt.params)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.wantErr)
			}
		})
	}
}

func TestValidateBackup(t *testing.T) {
	tests := []struct {
		name    string
		params  *ASBackupParams
		wantErr bool
		errMsg  string
	}{
		{
			name: "Valid backup configuration with output file",
			params: &ASBackupParams{
				BackupParams: &models.Backup{
					OutputFile: "backup.asb",
				},
				CommonParams: &models.Common{
					Namespace: "test",
				},
			},
			wantErr: false,
		},
		{
			name: "Valid backup configuration with directory",
			params: &ASBackupParams{
				BackupParams: &models.Backup{},
				CommonParams: &models.Common{
					Directory: "backup-dir",
					Namespace: "test",
				},
			},
			wantErr: false,
		},
		{
			name: "Valid backup configuration with estimate",
			params: &ASBackupParams{
				BackupParams: &models.Backup{
					Estimate:        true,
					EstimateSamples: 100,
				},
				CommonParams: &models.Common{
					Namespace: "test",
				},
			},
			wantErr: false,
		},
		{
			name: "Valid backup configuration with BackupXDR",
			params: &ASBackupParams{
				BackupXDRParams: &models.BackupXDR{
					MaxConnections: 10,
					FileLimit:      1000,
				},
			},
			wantErr: false,
		},
		{
			name: "Missing output file and directory",
			params: &ASBackupParams{
				BackupParams: &models.Backup{},
				CommonParams: &models.Common{
					Namespace: "test",
				},
			},
			wantErr: true,
			errMsg:  "output file or directory required",
		},
		{
			name: "Invalid backup params - both output file and directory",
			params: &ASBackupParams{
				BackupParams: &models.Backup{
					OutputFile: "backup.asb",
				},
				CommonParams: &models.Common{
					Directory: "backup-dir",
					Namespace: "test",
				},
			},
			wantErr: true,
			errMsg:  "only one of output-file and directory may be configured at the same time",
		},
		{
			name: "Invalid common params - missing namespace",
			params: &ASBackupParams{
				BackupParams: &models.Backup{
					OutputFile: "backup.asb",
				},
				CommonParams: &models.Common{},
			},
			wantErr: true,
			errMsg:  "namespace is required",
		},
		{
			name: "Invalid BackupXDR params",
			params: &ASBackupParams{
				BackupXDRParams: &models.BackupXDR{
					MaxConnections: 0, // Invalid: must be >= 1
				},
			},
			wantErr: true,
			errMsg:  "backup xdr max connections can't be less than 1",
		},
		{
			name: "Multiple cloud storage providers configured",
			params: &ASBackupParams{
				BackupParams: &models.Backup{
					OutputFile: "backup.asb",
				},
				CommonParams: &models.Common{
					Namespace: "test",
				},
				AwsS3: &models.AwsS3{
					Region: "us-west-2",
				},
				GcpStorage: &models.GcpStorage{
					BucketName: "my-bucket",
				},
			},
			wantErr: true,
			errMsg:  "only one cloud provider can be configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBackup(tt.params)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Equal(t, tt.errMsg, err.Error())
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateRestore(t *testing.T) {
	tests := []struct {
		name    string
		params  *ASRestoreParams
		wantErr bool
		errMsg  string
	}{
		{
			name: "Valid restore configuration with input file",
			params: &ASRestoreParams{
				RestoreParams: &models.Restore{
					InputFile: "backup.asb",
					Mode:      models.RestoreModeASB,
				},
				CommonParams: &models.Common{
					Namespace: "test",
				},
			},
			wantErr: false,
		},
		{
			name: "Valid restore configuration with directory",
			params: &ASRestoreParams{
				RestoreParams: &models.Restore{
					Mode: models.RestoreModeASB,
				},
				CommonParams: &models.Common{
					Directory: "restore-dir",
					Namespace: "test",
				},
			},
			wantErr: false,
		},
		{
			name: "Valid restore configuration with directory list",
			params: &ASRestoreParams{
				RestoreParams: &models.Restore{
					DirectoryList:   "dir1,dir2",
					ParentDirectory: "parent",
					Mode:            models.RestoreModeASB,
				},
				CommonParams: &models.Common{
					Namespace: "test",
				},
			},
			wantErr: false,
		},
		{
			name: "Invalid restore mode",
			params: &ASRestoreParams{
				RestoreParams: &models.Restore{
					InputFile: "backup.asb",
					Mode:      "invalid-mode",
				},
				CommonParams: &models.Common{
					Namespace: "test",
				},
			},
			wantErr: true,
			errMsg:  "invalid restore mode: invalid-mode",
		},
		{
			name: "Missing input source",
			params: &ASRestoreParams{
				RestoreParams: &models.Restore{
					Mode: models.RestoreModeASB,
				},
				CommonParams: &models.Common{
					Namespace: "test",
				},
			},
			wantErr: true,
			errMsg:  "input file or directory required",
		},
		{
			name: "Invalid restore params - both input file and directory",
			params: &ASRestoreParams{
				RestoreParams: &models.Restore{
					InputFile: "backup.asb",
					Mode:      models.RestoreModeASB,
				},
				CommonParams: &models.Common{
					Directory: "restore-dir",
					Namespace: "test",
				},
			},
			wantErr: true,
			errMsg:  "only one of directory and input-file may be configured at the same time",
		},
		{
			name: "Invalid common params - missing namespace",
			params: &ASRestoreParams{
				RestoreParams: &models.Restore{
					InputFile: "backup.asb",
					Mode:      models.RestoreModeASB,
				},
				CommonParams: &models.Common{},
			},
			wantErr: true,
			errMsg:  "namespace is required",
		},
		{
			name: "Multiple cloud storage providers configured",
			params: &ASRestoreParams{
				RestoreParams: &models.Restore{
					InputFile: "backup.asb",
					Mode:      models.RestoreModeASB,
				},
				CommonParams: &models.Common{
					Namespace: "test",
				},
				AwsS3: &models.AwsS3{
					Region: "us-west-2",
				},
				AzureBlob: &models.AzureBlob{
					ContainerName: "my-container",
				},
			},
			wantErr: true,
			errMsg:  "only one cloud provider can be configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRestore(tt.params)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Equal(t, tt.errMsg, err.Error())
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
