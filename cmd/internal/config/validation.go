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
	"fmt"
	"sort"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/cmd/internal/models"
)

func ValidateBackup(params *BackupParams) error {
	if params.Backup != nil && params.Common != nil {
		if params.Backup.OutputFile == "" && params.Common.Directory == "" && !params.Backup.Estimate {
			return fmt.Errorf("output file or directory required")
		}

		if err := ValidateBackupParams(params.Backup, params.Common); err != nil {
			return err
		}

		if err := ValidateCommonParams(params.Common); err != nil {
			return err
		}
	}

	if params.BackupXDR != nil {
		if err := ValidateBackupXDRParams(params.BackupXDR); err != nil {
			return err
		}
	}

	if err := ValidateStorages(params.AwsS3, params.GcpStorage, params.AzureBlob); err != nil {
		return err
	}

	return nil
}

func ValidateBackupXDRParams(params *models.BackupXDR) error {
	if params.ReadTimeoutMilliseconds < 0 {
		return fmt.Errorf("backup xdr read timeout can't be negative")
	}

	if params.WriteTimeoutMilliseconds < 0 {
		return fmt.Errorf("backup xdr write timeout can't be negative")
	}

	if params.InfoPolingPeriodMilliseconds < 0 {
		return fmt.Errorf("backup xdr info poling period can't be negative")
	}

	if params.StartTimeoutMilliseconds < 0 {
		return fmt.Errorf("backup xdr start timeout can't be negative")
	}

	if params.ResultQueueSize < 0 {
		return fmt.Errorf("backup xdr result queue size can't be negative")
	}

	if params.AckQueueSize < 0 {
		return fmt.Errorf("backup xdr ack queue size can't be negative")
	}

	if params.MaxConnections < 1 {
		return fmt.Errorf("backup xdr max connections can't be less than 1")
	}

	if params.ParallelWrite < 0 {
		return fmt.Errorf("backup xdr parallel write can't be negative")
	}

	if params.FileLimit < 1 {
		return fmt.Errorf("backup xdr file limit can't be less than 1")
	}

	if params.InfoRetryIntervalMilliseconds < 0 {
		return fmt.Errorf("backup xdr info retry interval can't be negative")
	}

	if params.InfoRetriesMultiplier < 0 {
		return fmt.Errorf("backup xdr info retries multiplier can't be negative")
	}

	return nil
}

func ValidateRestore(params *RestoreParams) error {
	if params.Restore != nil && params.Common != nil {
		switch params.Restore.Mode {
		case models.RestoreModeAuto, models.RestoreModeASB, models.RestoreModeASBX:
			// ok.
		default:
			return fmt.Errorf("invalid restore mode: %s", params.Restore.Mode)
		}

		if params.Restore.InputFile == "" &&
			params.Common.Directory == "" &&
			params.Restore.DirectoryList == "" {
			return fmt.Errorf("input file or directory required")
		}

		if err := ValidateRestoreParams(params.Restore, params.Common); err != nil {
			return err
		}

		if err := ValidateCommonParams(params.Common); err != nil {
			return err
		}
	}

	if err := ValidateStorages(params.AwsS3, params.GcpStorage, params.AzureBlob); err != nil {
		return err
	}

	if params.AwsS3 != nil {
		if params.AwsS3.RestorePollDuration < 1 {
			return fmt.Errorf("restore poll duration can't be less than 1")
		}
	}

	if params.AzureBlob != nil {
		if params.AzureBlob.RestorePollDuration < 1 {
			return fmt.Errorf("rehydrate poll duration can't be less than 1")
		}
	}

	return nil
}

//nolint:gocyclo // Long validation function.
func ValidateStorages(
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
) error {
	var count int

	if awsS3 != nil && (awsS3.BucketName != "" || awsS3.Region != "" || awsS3.Profile != "" || awsS3.Endpoint != "") {
		if err := awsS3.Validate(); err != nil {
			return fmt.Errorf("failed to validate aws s3: %w", err)
		}

		count++
	}

	if gcpStorage != nil && (gcpStorage.BucketName != "" || gcpStorage.KeyFile != "" || gcpStorage.Endpoint != "") {
		if err := gcpStorage.Validate(); err != nil {
			return fmt.Errorf("failed to validate gcp storage: %w", err)
		}

		count++
	}

	if azureBlob != nil && (azureBlob.ContainerName != "" || azureBlob.AccountName != "" || azureBlob.AccountKey != "" ||
		azureBlob.Endpoint != "" || azureBlob.TenantID != "" || azureBlob.ClientID != "" ||
		azureBlob.ClientSecret != "") {
		if err := azureBlob.Validate(); err != nil {
			return fmt.Errorf("failed to validate azure blob: %w", err)
		}

		count++
	}

	if count > 1 {
		return fmt.Errorf("only one cloud provider can be configured")
	}

	return nil
}

//nolint:gocyclo // It is a long validation function.
func ValidateBackupParams(backupParams *models.Backup, commonParams *models.Common) error {
	if backupParams == nil || commonParams == nil {
		return fmt.Errorf("params can't be nil")
	}

	if commonParams.Directory != "" && backupParams.OutputFile != "" {
		return fmt.Errorf("only one of output-file and directory may be configured at the same time")
	}

	// Only one filter is allowed.
	if backupParams.AfterDigest != "" && backupParams.PartitionList != "" {
		return fmt.Errorf("only one of after-digest or partition-list can be configured")
	}

	if (backupParams.Continue != "" || backupParams.Estimate || backupParams.StateFileDst != "") &&
		(backupParams.ParallelNodes || backupParams.NodeList != "") {
		return fmt.Errorf("saving states and calculating estimates is not possible in parallel node mode")
	}

	if backupParams.Estimate {
		// Estimate with filter not allowed.
		if backupParams.PartitionList != "" ||
			backupParams.NodeList != "" ||
			backupParams.AfterDigest != "" ||
			backupParams.FilterExpression != "" ||
			backupParams.ModifiedAfter != "" ||
			backupParams.ModifiedBefore != "" ||
			backupParams.NoTTLOnly {
			return fmt.Errorf("estimate with any filter is not allowed")
		}
		// For estimate directory or file must not be set.
		if backupParams.OutputFile != "" || commonParams.Directory != "" {
			return fmt.Errorf("estimate with output-file or directory is not allowed")
		}
		// Check estimate samples size.
		if backupParams.EstimateSamples < 0 {
			return fmt.Errorf("estimate with estimate-samples < 0 is not allowed")
		}
	}

	if !backupParams.Estimate && backupParams.OutputFile == "" && commonParams.Directory == "" {
		return fmt.Errorf("must specify either output-file or directory")
	}

	if backupParams.NodeList != "" && backupParams.RackList != "" {
		return fmt.Errorf("specify either rack-list or node-list, but not both")
	}

	return nil
}

func ValidateCommonParams(commonParams *models.Common) error {
	if commonParams.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}

	if commonParams.TotalTimeout < 0 {
		return fmt.Errorf("total-timeout must be non-negative")
	}

	if commonParams.SocketTimeout < 0 {
		return fmt.Errorf("socket-timeout must be non-negative")
	}

	if commonParams.Parallel < 0 {
		return fmt.Errorf("parallel must be non-negative")
	}

	return nil
}

func validatePartitionFilters(partitionFilters []*aerospike.PartitionFilter) error {
	if len(partitionFilters) < 1 {
		return nil
	}

	beginMap := make(map[int]bool)
	intervals := make([][2]int, 0)

	for _, filter := range partitionFilters {
		switch {
		case filter.Count == 1:
			if beginMap[filter.Begin] {
				return fmt.Errorf("duplicate begin value %d for count = 1", filter.Begin)
			}

			beginMap[filter.Begin] = true
		case filter.Count > 1:
			begin := filter.Begin
			end := filter.Begin + filter.Count
			intervals = append(intervals, [2]int{begin, end})
		default:
			return fmt.Errorf("invalid partition filter count: %d", filter.Count)
		}
	}

	sort.Slice(intervals, func(i, j int) bool {
		return intervals[i][0] < intervals[j][0]
	})

	for i := 1; i < len(intervals); i++ {
		prevEnd := intervals[i-1][1]
		currBegin := intervals[i][0]

		if currBegin <= prevEnd {
			return fmt.Errorf("overlapping intervals: [%d, %d] and [%d, %d]",
				intervals[i-1][0], prevEnd, currBegin, intervals[i][1])
		}
	}

	return nil
}

func ValidateRestoreParams(restoreParams *models.Restore, commonParams *models.Common) error {
	if commonParams.Directory != "" && restoreParams.InputFile != "" {
		return fmt.Errorf("only one of directory and input-file may be configured at the same time")
	}

	if restoreParams.DirectoryList != "" && (commonParams.Directory != "" || restoreParams.InputFile != "") {
		return fmt.Errorf("only one of directory, input-file and directory-list may be configured at the same time")
	}

	if restoreParams.ParentDirectory != "" && restoreParams.DirectoryList == "" {
		return fmt.Errorf("must specify directory-list list")
	}

	if restoreParams.WarmUp < 0 {
		return fmt.Errorf("warm-up must be non-negative")
	}

	return nil
}
