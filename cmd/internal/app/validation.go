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

func validateBackupParams(cfg *models.Backup) error {
	if cfg.AfterDigest != "" && cfg.PartitionList != "" {
		return fmt.Errorf("only one of after-digest or partition-list can be configured")
	}

	return nil
}

func validatePartitionFilters(partitionFilters []*aerospike.PartitionFilter) error {
	if len(partitionFilters) <= 1 {
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
			end := filter.Begin + filter.Count - 1
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
