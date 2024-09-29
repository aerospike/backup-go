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

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/stretchr/testify/assert"
)

func TestSplitPartitions_SinglePartitionRange(t *testing.T) {
	t.Parallel()

	partitionFilters := []*aerospike.PartitionFilter{
		{Begin: 0, Count: 100},
	}
	numWorkers := 5

	result, err := splitPartitions(partitionFilters, numWorkers)

	assert.NoError(t, err)
	assert.Len(t, result, numWorkers, "The result should contain 5 split partitions")

	for i := 0; i < numWorkers; i++ {
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

	assert.NoError(t, err)
	assert.Len(t, result, numWorkers, "The result should contain 4 split partitions")

	expectedResults := []*aerospike.PartitionFilter{
		{Begin: 0, Count: 25},
		{Begin: 25, Count: 25},
		{Begin: 100, Count: 25},
		{Begin: 125, Count: 25},
	}
	for i := 0; i < len(expectedResults); i++ {
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

	assert.NoError(t, err)
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

	assert.NoError(t, err)
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

	assert.Error(t, err)
	assert.Equal(t, "numWorkers is less than partitionFilters, cannot split partitionFilters", err.Error())
}

func TestSplitPartitionRange(t *testing.T) {
	t.Parallel()

	partitionFilter := &aerospike.PartitionFilter{Begin: 0, Count: 100}
	numWorkers := 5

	result := splitPartitionRange(partitionFilter, numWorkers)

	assert.Len(t, result, numWorkers, "The result should contain 5 split partitions")

	for i := 0; i < numWorkers; i++ {
		assert.Equal(t, i*20, result[i].Begin)
		assert.Equal(t, 20, result[i].Count)
	}
}
