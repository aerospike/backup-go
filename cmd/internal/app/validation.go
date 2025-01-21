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
	"sort"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/cmd/internal/models"
)

func validateBackup(params *ASBackupParams) error {
	if params.BackupParams != nil && params.CommonParams != nil {
		if params.BackupParams.OutputFile == "" && params.CommonParams.Directory == "" {
			return fmt.Errorf("output file or directory required")
		}

		if err := validateBackupParams(params.BackupParams, params.CommonParams); err != nil {
			return err
		}

		if err := validateCommonParams(params.CommonParams); err != nil {
			return err
		}
	}

	if err := validateStorages(params.AwsS3, params.GcpStorage, params.AzureBlob); err != nil {
		return err
	}

	return nil
}

func validateRestore(params *ASRestoreParams) error {
	if params.RestoreParams != nil && params.CommonParams != nil {
		switch params.RestoreParams.Mode {
		case models.RestoreModeAuto, models.RestoreModeASB, models.RestoreModeASBX:
			// ok.
		default:
			return fmt.Errorf("invalid restore mode: %s", params.RestoreParams.Mode)
		}

		if params.RestoreParams.InputFile == "" &&
			params.CommonParams.Directory == "" &&
			params.RestoreParams.DirectoryList == "" {
			return fmt.Errorf("input file or directory required")
		}

		if err := validateRestoreParams(params.RestoreParams, params.CommonParams); err != nil {
			return err
		}

		if err := validateCommonParams(params.CommonParams); err != nil {
			return err
		}
	}

	if err := validateStorages(params.AwsS3, params.GcpStorage, params.AzureBlob); err != nil {
		return err
	}

	return nil
}

func validateStorages(
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
) error {
	var count int

	if awsS3 != nil && (awsS3.Region != "" || awsS3.Profile != "" || awsS3.Endpoint != "") {
		count++
	}

	if gcpStorage != nil && (gcpStorage.BucketName != "" || gcpStorage.KeyFile != "" || gcpStorage.Endpoint != "") {
		count++
	}

	if azureBlob != nil && (azureBlob.ContainerName != "" || azureBlob.AccountName != "" || azureBlob.AccountKey != "" ||
		azureBlob.Endpoint != "" || azureBlob.TenantID != "" || azureBlob.ClientID != "" ||
		azureBlob.ClientSecret != "") {
		count++
	}

	if count > 1 {
		return fmt.Errorf("only one cloud provider can be configured")
	}

	return nil
}

//nolint:gocyclo // It is a long validation function.
func validateBackupParams(backupParams *models.Backup, commonParams *models.Common) error {
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

	return nil
}

func validateCommonParams(commonParams *models.Common) error {
	if commonParams.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}

	if commonParams.TotalTimeout < 0 {
		return fmt.Errorf("total-timeout must be non-negative")
	}

	if commonParams.SocketTimeout < 0 {
		return fmt.Errorf("socket-timeout must be non-negative")
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

func validateRestoreParams(restoreParams *models.Restore, commonParams *models.Common) error {
	if commonParams.Directory != "" && restoreParams.InputFile != "" {
		return fmt.Errorf("only one of directory and input-file may be configured at the same time")
	}

	if restoreParams.DirectoryList != "" && (commonParams.Directory != "" || restoreParams.InputFile != "") {
		return fmt.Errorf("only one of directory, input-file and directory-list may be configured at the same time")
	}

	if restoreParams.ParentDirectory != "" && restoreParams.DirectoryList == "" {
		return fmt.Errorf("must specify directory-list list")
	}

	return nil
}
