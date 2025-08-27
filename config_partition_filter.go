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
	"regexp"
	"strconv"
	"strings"

	a "github.com/aerospike/aerospike-client-go/v8"
)

var (
	//nolint:lll // The regexp is long.
	expPartitionRange  = regexp.MustCompile(`^([0-9]|[1-9][0-9]{1,3}|40[0-8][0-9]|409[0-5])\-([1-9]|[1-9][0-9]{1,3}|40[0-8][0-9]|409[0-6])$`)
	expPartitionID     = regexp.MustCompile(`^(409[0-6]|40[0-8]\d|[123]?\d{1,3}|0)$`)
	expPartitionDigest = regexp.MustCompile(`^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$`)
)

// NewPartitionFilterByRange returns a partition range with boundaries specified by the provided values.
func NewPartitionFilterByRange(begin, count int) *a.PartitionFilter {
	return a.NewPartitionFilterByRange(begin, count)
}

// NewPartitionFilterByID returns a partition filter by id with specified id.
func NewPartitionFilterByID(partitionID int) *a.PartitionFilter {
	return a.NewPartitionFilterById(partitionID)
}

// NewPartitionFilterByIDs returns a partition filter by ids with specified ids.
func NewPartitionFilterByIDs(partitionIDs []int) (*a.PartitionFilter, error) {
	return a.NewPartitionFilterSelectPartitions(partitionIDs)
}

// NewPartitionFilterByDigest returns a partition filter by digest with specified value.
func NewPartitionFilterByDigest(namespace, digest string) (*a.PartitionFilter, error) {
	key, err := newKeyByDigest(namespace, digest)
	if err != nil {
		return nil, err
	}

	pf := a.NewPartitionFilterByKey(key)
	// We must nullify digest, to start partition from the beginning.
	pf.Digest = nil

	return pf, nil
}

// NewPartitionFilterAfterDigest returns a partition filter to scan records after the digest.
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

// splitPartitions splits the partitions to groups.
func splitPartitions(partitionFilters []*a.PartitionFilter, numWorkers int) ([]*a.PartitionFilter, error) {
	if numWorkers < 1 {
		return nil, fmt.Errorf("number of workers is less than 1, cannot split partition filters")
	}

	if numWorkers < len(partitionFilters) {
		return nil, fmt.Errorf("number of workers is less than partition filters, cannot split partition filters")
	}

	// Validations.
	for i := range partitionFilters {
		if partitionFilters[i].Begin < 0 {
			return nil, fmt.Errorf("start partition is less than 0, cannot split partition filters")
		}

		if partitionFilters[i].Count < 1 {
			return nil, fmt.Errorf("partitions count is less than 1, cannot split partition filters")
		}

		if partitionFilters[i].Begin+partitionFilters[i].Count > MaxPartitions {
			return nil, fmt.Errorf("start partition + partitions count is greater than the max partition filters: %d",
				MaxPartitions)
		}
	}

	// If we have one partition filter with range.
	if len(partitionFilters) == 1 && partitionFilters[0].Count != 1 {
		return splitPartitionRange(partitionFilters[0], numWorkers)
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

		group, err := splitPartitionRange(filter, numRangeWorkers)
		if err != nil {
			return nil, err
		}

		result = append(result, group...)

		remainingWorkers -= numRangeWorkers
	}

	return result, nil
}

func splitPartitionIDs(ids []int, numWorkers int) ([]*a.PartitionFilter, error) {
	var err error
	result := make([]*a.PartitionFilter, numWorkers)

	baseSize := len(ids) / numWorkers
	remainder := len(ids) % numWorkers
	startIdx := 0

	for i := 0; i < numWorkers; i++ {
		currentSize := baseSize
		if i < remainder {
			currentSize++
		}

		endIdx := startIdx + currentSize
		result[i], err = NewPartitionFilterByIDs(ids[startIdx:endIdx])
		if err != nil {
			return nil, fmt.Errorf("failed to split partition ids: %w", err)
		}
		startIdx = endIdx
	}

	return result, nil
}

// splitPartitionRange splits one range filter to numWorkers.
func splitPartitionRange(partitionFilters *a.PartitionFilter, numWorkers int) ([]*a.PartitionFilter, error) {
	if partitionFilters.Count < numWorkers {
		return nil, fmt.Errorf("number of partitions is less than workers number, cannot split partitions")
	}

	result := make([]*a.PartitionFilter, numWorkers)
	for j := 0; j < numWorkers; j++ {
		result[j] = &a.PartitionFilter{}
		result[j].Begin = (j * partitionFilters.Count) / numWorkers
		result[j].Count = (((j + 1) * partitionFilters.Count) / numWorkers) - result[j].Begin
		result[j].Begin += partitionFilters.Begin
		// Set digest property for the first group.
		if partitionFilters.Digest != nil && j == 0 {
			result[j].Digest = partitionFilters.Digest
		}
	}

	return result, nil
}

// ParsePartitionFilterListString parses comma separated values to slice of partition filters.
// Example: "0-1000,1000-1000,2222,EjRWeJq83vEjRRI0VniavN7xI0U="
// Namespace can be empty, must be set only for partition by digest.
func ParsePartitionFilterListString(namespace, filters string) ([]*a.PartitionFilter, error) {
	if filters == "" {
		return nil, fmt.Errorf("empty filters")
	}

	filterSlice := strings.Split(filters, ",")
	partitionFilters := make([]*a.PartitionFilter, 0, len(filterSlice))

	for i := range filterSlice {
		partitionFilter, err := ParsePartitionFilterString(namespace, filterSlice[i])
		if err != nil {
			return nil, fmt.Errorf("failed to parse partition filter, filter: %s, err: %v", filterSlice[i], err)
		}

		partitionFilters = append(partitionFilters, partitionFilter)
	}

	return partitionFilters, nil
}

// ParsePartitionFilterString check inputs from string with regexp.
// Parse values and returns *aerospike.PartitionFilter or error.
// Namespace can be empty, must be set only for partition by digest.
func ParsePartitionFilterString(namespace, filter string) (*a.PartitionFilter, error) {
	// Range 0-4096
	if expPartitionRange.MatchString(filter) {
		return parsePartitionFilterByRange(filter)
	}

	// Id 1456
	if expPartitionID.MatchString(filter) {
		return parsePartitionFilterByID(filter)
	}

	// Digest (base64 string)
	if expPartitionDigest.MatchString(filter) {
		return parsePartitionFilterByDigest(namespace, filter)
	}

	return nil, fmt.Errorf("failed to parse partition filter: %s", filter)
}

func parsePartitionFilterByRange(filter string) (*a.PartitionFilter, error) {
	bounds := strings.Split(filter, "-")
	if len(bounds) != 2 {
		return nil, fmt.Errorf("invalid partition filter: %s", filter)
	}

	begin, err := strconv.Atoi(bounds[0])
	if err != nil {
		return nil, fmt.Errorf("invalid partition filter %s begin value: %w", filter, err)
	}

	count, err := strconv.Atoi(bounds[1])
	if err != nil {
		return nil, fmt.Errorf("invalid partition filter %s count value: %w", filter, err)
	}

	return NewPartitionFilterByRange(begin, count), nil
}

func parsePartitionFilterByID(filter string) (*a.PartitionFilter, error) {
	id, err := strconv.Atoi(filter)
	if err != nil {
		return nil, fmt.Errorf("invalid partition filter %s id value: %w", filter, err)
	}

	return NewPartitionFilterByID(id), nil
}

func parsePartitionFilterByDigest(namespace, filter string) (*a.PartitionFilter, error) {
	return NewPartitionFilterByDigest(namespace, filter)
}
