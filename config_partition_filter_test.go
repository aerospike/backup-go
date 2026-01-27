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
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitPartitions_SinglePartitionRange(t *testing.T) {
	t.Parallel()

	partitionFilters := []*aerospike.PartitionFilter{
		{Begin: 0, Count: 100},
	}
	numWorkers := 5

	result, err := splitPartitions(partitionFilters, numWorkers)

	require.NoError(t, err)
	assert.Len(t, result, numWorkers, "The result should contain 5 split partitions")

	for i := range numWorkers {
		assert.Equal(t, i*20, result[i].Begin)
		assert.Equal(t, 20, result[i].Count)
	}
}

func TestSplitPartitions_MultiplePartitionsRange(t *testing.T) {
	t.Parallel()

	partitionFilters := []*aerospike.PartitionFilter{
		{Begin: 0, Count: 50},
		{Begin: 100, Count: 50},
	}
	numWorkers := 4

	result, err := splitPartitions(partitionFilters, numWorkers)

	require.NoError(t, err)
	assert.Len(t, result, numWorkers, "The result should contain 4 split partitions")

	expectedResults := []*aerospike.PartitionFilter{
		{Begin: 0, Count: 25},
		{Begin: 25, Count: 25},
		{Begin: 100, Count: 25},
		{Begin: 125, Count: 25},
	}
	for i := range expectedResults {
		assert.Equal(t, expectedResults[i].Begin, result[i].Begin)
		assert.Equal(t, expectedResults[i].Count, result[i].Count)
	}
}

func TestSplitPartitions_SingleCountFilters(t *testing.T) {
	t.Parallel()

	partitionFilters := []*aerospike.PartitionFilter{
		{Begin: 0, Count: 1},
		{Begin: 2, Count: 1},
		{Begin: 4, Count: 1},
	}
	numWorkers := 3

	result, err := splitPartitions(partitionFilters, numWorkers)

	require.NoError(t, err)
	assert.Len(t, result, numWorkers, "The result should contain 3 partitions")

	for i := range result {
		assert.Equal(t, 1, result[i].Count)
	}
}

func TestSplitPartitions_MixedFilters(t *testing.T) {
	t.Parallel()

	partitionFilters := []*aerospike.PartitionFilter{
		{Begin: 0, Count: 10},
		{Begin: 20, Count: 1},
		{Begin: 30, Count: 15},
	}
	numWorkers := 5

	result, err := splitPartitions(partitionFilters, numWorkers)

	require.NoError(t, err)
	assert.Len(t, result, numWorkers, "The result should contain 5 partitions")

	assert.Equal(t, 20, result[0].Begin)

	assert.Equal(t, 0, result[1].Begin)
	assert.Equal(t, 10, result[1].Count)

	assert.Equal(t, 30, result[2].Begin)
	assert.Equal(t, 5, result[2].Count)

	assert.Equal(t, 35, result[3].Begin)
	assert.Equal(t, 5, result[3].Count)

	assert.Equal(t, 40, result[4].Begin)
	assert.Equal(t, 5, result[4].Count)
}

func TestSplitPartitions_NumWorkersLessThanFilters(t *testing.T) {
	t.Parallel()

	partitionFilters := []*aerospike.PartitionFilter{
		{Begin: 0, Count: 10},
		{Begin: 10, Count: 5},
		{Begin: 20, Count: 15},
	}
	numWorkers := 2

	_, err := splitPartitions(partitionFilters, numWorkers)

	require.Error(t, err)
	assert.Equal(t, "number of workers is less than partition filters, cannot split partition filters", err.Error())
}

func TestSplitPartitionRange(t *testing.T) {
	t.Parallel()

	partitionFilter := &aerospike.PartitionFilter{Begin: 0, Count: 100}
	numWorkers := 5

	result, err := splitPartitionRange(partitionFilter, numWorkers)
	require.NoError(t, err)

	assert.Len(t, result, numWorkers, "The result should contain 5 split partitions")

	for i := range numWorkers {
		assert.Equal(t, i*20, result[i].Begin)
		assert.Equal(t, 20, result[i].Count)
	}
}

func TestParsePartitionFilterByRange_Valid(t *testing.T) {
	t.Parallel()
	filter := "100-200"
	parsedFilter, err := parsePartitionFilterByRange(filter)
	require.NoError(t, err)
	assert.NotNil(t, parsedFilter)
}

func TestParsePartitionFilterByRange_InvalidRange(t *testing.T) {
	t.Parallel()
	filter := "invalid-range"
	parsedFilter, err := parsePartitionFilterByRange(filter)
	require.Error(t, err)
	assert.Nil(t, parsedFilter)
	assert.Contains(t, err.Error(), "invalid partition filter")
}

func TestParsePartitionFilterByID_Valid(t *testing.T) {
	t.Parallel()
	filter := "1234"
	parsedFilter, err := parsePartitionFilterByID(filter)
	require.NoError(t, err)
	assert.NotNil(t, parsedFilter)
}

func TestParsePartitionFilterByID_InvalidID(t *testing.T) {
	t.Parallel()
	filter := "invalid-id"
	parsedFilter, err := parsePartitionFilterByID(filter)
	require.Error(t, err)
	assert.Nil(t, parsedFilter)
	assert.Contains(t, err.Error(), "invalid partition filter")
}

func TestParsePartitionFilterByDigest_Valid(t *testing.T) {
	t.Parallel()
	namespace := "test-namespace"
	filter := "EjRWeJq83vEjRRI0VniavN7xI0U=" // Base64-encoded digest
	parsedFilter, err := parsePartitionFilterByDigest(namespace, filter)
	require.NoError(t, err)
	assert.NotNil(t, parsedFilter)
}

func TestParsePartitionFilterByDigest_InvalidDigest(t *testing.T) {
	t.Parallel()
	namespace := "test-namespace"
	filter := "invalid-digest"
	parsedFilter, err := parsePartitionFilterByDigest(namespace, filter)
	require.Error(t, err)
	assert.Nil(t, parsedFilter)
	assert.Contains(t, err.Error(), "failed to decode after-digest")
}

func TestParsePartitionFilter_InvalidFilter(t *testing.T) {
	t.Parallel()
	namespace := "test-namespace"
	filter := "invalid-filter"
	parsedFilter, err := ParsePartitionFilterString(namespace, filter)
	require.Error(t, err)
	assert.Nil(t, parsedFilter)
	assert.Contains(t, err.Error(), "failed to parse partition filter")
}

func TestParsePartitionFilterListString_Valid(t *testing.T) {
	t.Parallel()
	namespace := "test-namespace"
	filter := "0-1000,1000-1000,2222,EjRWeJq83vEjRRI0VniavN7xI0U="
	parsedFilter, err := ParsePartitionFilterListString(namespace, filter)
	require.NoError(t, err)
	assert.NotNil(t, parsedFilter)
}

func TestParsePartitionFilterListString_Empty(t *testing.T) {
	t.Parallel()
	namespace := "test-namespace"
	filter := ""
	parsedFilter, err := ParsePartitionFilterListString(namespace, filter)
	require.Error(t, err)
	assert.Nil(t, parsedFilter)
	assert.Contains(t, err.Error(), "empty filters")
}

func TestParsePartitionFilterListString_Err(t *testing.T) {
	t.Parallel()
	namespace := "test-namespace"
	filter := "EjRWeJq83vEjR"
	parsedFilter, err := ParsePartitionFilterListString(namespace, filter)
	require.Error(t, err)
	assert.Nil(t, parsedFilter)
	assert.Contains(t, err.Error(), "failed to parse partition filter")
}

func TestSplitPartitionRange_ErrNumPartLessWorkers(t *testing.T) {
	t.Parallel()

	partitionFilter := &aerospike.PartitionFilter{Begin: 0, Count: 4}
	numWorkers := 5

	result, err := splitPartitionRange(partitionFilter, numWorkers)
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "number of partitions is less than workers number, cannot split partitions")
}

func TestSplitPartition_ErrNumWorkersLess1(t *testing.T) {
	t.Parallel()

	partitionFilters := []*aerospike.PartitionFilter{
		{Begin: 0, Count: 10},
		{Begin: 10, Count: 5},
		{Begin: 20, Count: 15},
	}
	numWorkers := 0

	result, err := splitPartitions(partitionFilters, numWorkers)
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "number of workers is less than 1, cannot split partition filters")
}

func TestPartitionFilter_splitPartitionIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ids        []int
		numWorkers int
		result     []*aerospike.PartitionFilter
		wantErr    bool
	}{
		{
			name:       "success",
			ids:        []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			numWorkers: 10,
			result: []*aerospike.PartitionFilter{
				newTestPartitionFilterByIDs([]int{0}),
				newTestPartitionFilterByIDs([]int{1}),
				newTestPartitionFilterByIDs([]int{2}),
				newTestPartitionFilterByIDs([]int{3}),
				newTestPartitionFilterByIDs([]int{4}),
				newTestPartitionFilterByIDs([]int{5}),
				newTestPartitionFilterByIDs([]int{6}),
				newTestPartitionFilterByIDs([]int{7}),
				newTestPartitionFilterByIDs([]int{8}),
				newTestPartitionFilterByIDs([]int{9}),
			},
			wantErr: false,
		},
		{
			name:       "success small workers",
			ids:        []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			numWorkers: 5,
			result: []*aerospike.PartitionFilter{
				newTestPartitionFilterByIDs([]int{0, 1}),
				newTestPartitionFilterByIDs([]int{2, 3}),
				newTestPartitionFilterByIDs([]int{4, 5}),
				newTestPartitionFilterByIDs([]int{6, 7}),
				newTestPartitionFilterByIDs([]int{8, 9}),
			},
			wantErr: false,
		},
		{
			name:       "num workers error",
			ids:        []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			numWorkers: -1,
			result:     nil,
			wantErr:    true,
		},
		{
			name:       "len ids error",
			ids:        []int{},
			numWorkers: 10,
			result:     nil,
			wantErr:    true,
		},
		{
			name:       "max part error",
			ids:        make([]int, 5000),
			numWorkers: 10,
			result:     nil,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := splitPartitionIDs(tt.ids, tt.numWorkers)
			if tt.wantErr {
				require.Error(t, err)
			}
			assert.EqualExportedValues(t, tt.result, result)
		})
	}
}

func newTestPartitionFilterByIDs(slice []int) *aerospike.PartitionFilter {
	result, _ := NewPartitionFilterByIDs(slice)
	return result
}
