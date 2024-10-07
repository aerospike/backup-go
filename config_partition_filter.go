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

package backup

import (
	"fmt"

	a "github.com/aerospike/aerospike-client-go/v7"
)

// NewPartitionFilterByRange returns a partition range with boundaries specified by the provided values.
func NewPartitionFilterByRange(begin, count int) *a.PartitionFilter {
	return a.NewPartitionFilterByRange(begin, count)
}

// NewPartitionFilterByID returns a partition filter by id with specified id.
func NewPartitionFilterByID(partitionID int) *a.PartitionFilter {
	return a.NewPartitionFilterById(partitionID)
}

// NewPartitionFilterByDigest returns a partition filter by digest with specified value.
func NewPartitionFilterByDigest(namespace, digest string) (*a.PartitionFilter, error) {
	key, err := newKeyByDigest(namespace, digest)
	if err != nil {
		return nil, err
	}

	return a.NewPartitionFilterByKey(key), nil
}

// NewPartitionFilterAfterDigest returns partition filter to scan call records after digest.
func NewPartitionFilterAfterDigest(namespace, digest string) (*a.PartitionFilter, error) {
	key, err := newKeyByDigest(namespace, digest)
	if err != nil {
		return nil, err
	}

	defaultFilter := NewPartitionFilterAll()
	begin := key.PartitionId()
	count := defaultFilter.Count - begin

	return &a.PartitionFilter{
		Begin:  begin,
		Count:  count,
		Digest: key.Digest(),
	}, nil
}

// NewPartitionFilterAll returns a partition range containing all partitions.
func NewPartitionFilterAll() *a.PartitionFilter {
	return a.NewPartitionFilterByRange(0, MaxPartitions)
}

// splitPartitions splits partition to groups.
func splitPartitions(partitionFilters []*a.PartitionFilter, numWorkers int) ([]*a.PartitionFilter, error) {
	if numWorkers < 1 || numWorkers < len(partitionFilters) {
		return nil, fmt.Errorf("numWorkers is less than PartitionFilters, cannot split PartitionFilters")
	}

	// Validations.
	for i := range partitionFilters {
		if partitionFilters[i].Begin < 0 {
			return nil, fmt.Errorf("startPartition is less than 0, cannot split PartitionFilters")
		}

		if partitionFilters[i].Count < 1 {
			return nil, fmt.Errorf("numPartitions is less than 1, cannot split PartitionFilters")
		}

		if partitionFilters[i].Begin+partitionFilters[i].Count > MaxPartitions {
			return nil, fmt.Errorf("startPartition + numPartitions is greater than the max PartitionFilters: %d",
				MaxPartitions)
		}
	}

	// If we have one partition filter with range.
	if len(partitionFilters) == 1 && partitionFilters[0].Count != 1 && partitionFilters[0].Digest == nil {
		return splitPartitionRange(partitionFilters[0], numWorkers), nil
	}

	// If the same amount of partition filters, we distribute them to workers 1=1.
	if len(partitionFilters) == numWorkers {
		return partitionFilters, nil
	}

	// If we have more workers than filters.
	allWorkers := numWorkers

	filtersWithSingle := make([]*a.PartitionFilter, 0)
	filtersWithRange := make([]*a.PartitionFilter, 0)
	// Spread partitions by groups.
	for _, filter := range partitionFilters {
		switch filter.Count {
		case 1:
			filtersWithSingle = append(filtersWithSingle, filter)
		default:
			filtersWithRange = append(filtersWithRange, filter)
		}
	}

	// If single filters == numWorkers, return result.
	if len(filtersWithSingle) > 0 {
		// If we don't have range filters, we return single filters.
		if len(filtersWithRange) == 0 {
			return filtersWithSingle, nil
		}

		allWorkers -= len(filtersWithSingle)
	}

	// Now, distribute remaining workers to filters with Count > 1
	var totalRangeCount int
	for _, filter := range filtersWithRange {
		totalRangeCount += filter.Count
	}

	// Split remaining workers between range filters proportionally
	result := make([]*a.PartitionFilter, 0, numWorkers)
	result = append(result, filtersWithSingle...)

	remainingWorkers := allWorkers

	for i, filter := range filtersWithRange {
		numRangeWorkers := (filter.Count * allWorkers) / totalRangeCount
		if numRangeWorkers == 0 {
			numRangeWorkers = 1
		}
		// for the last range we give all remaining workers
		if i == len(filtersWithRange)-1 {
			numRangeWorkers = remainingWorkers
		}

		result = append(result, splitPartitionRange(filter, numRangeWorkers)...)

		remainingWorkers -= numRangeWorkers
	}

	return result, nil
}

// splitPartitionRange splits one range filter to numWorkers
func splitPartitionRange(partitionFilters *a.PartitionFilter, numWorkers int) []*a.PartitionFilter {
	result := make([]*a.PartitionFilter, numWorkers)
	for j := 0; j < numWorkers; j++ {
		result[j] = &a.PartitionFilter{}
		result[j].Begin = (j * partitionFilters.Count) / numWorkers
		result[j].Count = (((j + 1) * partitionFilters.Count) / numWorkers) - result[j].Begin
		result[j].Begin += partitionFilters.Begin
	}

	return result
}
