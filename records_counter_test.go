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
	"context"
	"errors"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCountUsingInfoClient(t *testing.T) {
	tests := []struct {
		name        string
		namespace   string
		partFilters []*a.PartitionFilter
		recordCount uint64
		infoError   error
		expected    uint64
		expectError bool
	}{
		{
			name:        "Successfully get record count",
			namespace:   "test",
			partFilters: []*a.PartitionFilter{a.NewPartitionFilterAll()},
			recordCount: 1000,
			expected:    1000,
			expectError: false,
		},
		{
			name:        "Successfully get record count for 1 partition",
			namespace:   "test",
			partFilters: []*a.PartitionFilter{a.NewPartitionFilterById(1)},
			recordCount: 8192,
			expected:    2,
			expectError: false,
		}, {
			name:      "Successfully get record count for multiple partitions",
			namespace: "test",
			partFilters: []*a.PartitionFilter{
				a.NewPartitionFilterById(1),
				a.NewPartitionFilterByRange(10, 10),
				a.NewPartitionFilterByRange(100, 100),
				a.NewPartitionFilterByRange(1000, 913),
			},
			recordCount: 4000,
			expected:    1000, // 1 + 10 + 100 + 913 = 1024 (1/4 of 4096)
			expectError: false,
		},
		{
			name:        "Successfully get record count with partial partition scan",
			namespace:   "test",
			partFilters: []*a.PartitionFilter{{Begin: 0, Count: MaxPartitions / 2}},
			recordCount: 1000,
			expected:    500,
			expectError: false,
		},
		{
			name:        "Handle info client error",
			namespace:   "test",
			partFilters: []*a.PartitionFilter{{Begin: 0, Count: MaxPartitions}},
			recordCount: 0,
			infoError:   errors.New("info error"),
			expected:    0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockInfoClient := mocks.NewMockInfoGetter(t)
			mockInfoClient.On("GetRecordCount", tt.namespace, []string{"set1"}).Return(tt.recordCount, tt.infoError)

			handler := &recordCounter{
				config: &ConfigBackup{
					Namespace:        tt.namespace,
					SetList:          []string{"set1"},
					PartitionFilters: tt.partFilters,
				},
			}

			ctx := context.Background()

			result, err := handler.countUsingInfoClient(ctx, mockInfoClient)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}

			mockInfoClient.AssertExpectations(t)
		})
	}
}

func TestRandomPartition(t *testing.T) {
	tests := []struct {
		name               string
		partitionFilters   []*a.PartitionFilter
		expectedBeginRange []int
		expectedCount      int
	}{
		{
			name:               "Empty filter list returns random partition",
			partitionFilters:   []*a.PartitionFilter{},
			expectedBeginRange: []int{0, MaxPartitions - 1},
			expectedCount:      1,
		},
		{
			name: "Single filter returns partition within range",
			partitionFilters: []*a.PartitionFilter{
				{Begin: 100, Count: 10},
			},
			expectedBeginRange: []int{100, 109},
			expectedCount:      1,
		},
		{
			name: "Multiple filters returns partition from one of them",
			partitionFilters: []*a.PartitionFilter{
				{Begin: 100, Count: 10},
				{Begin: 200, Count: 20},
				{Begin: 300, Count: 30},
			},
			expectedBeginRange: []int{0, 0},
			expectedCount:      1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := randomPartition(tt.partitionFilters)

			// Check that result is a valid partition filter
			require.NotNil(t, result)
			require.Equal(t, tt.expectedCount, result.Count)

			switch len(tt.partitionFilters) {
			case 0:
				// For empty filter list, check that result is within valid range
				require.GreaterOrEqual(t, result.Begin, tt.expectedBeginRange[0])
				require.LessOrEqual(t, result.Begin, tt.expectedBeginRange[1])
			case 1:
				// For single filter, check that result is within that filter's range
				require.GreaterOrEqual(t, result.Begin, tt.expectedBeginRange[0])
				require.LessOrEqual(t, result.Begin, tt.expectedBeginRange[1])
			default:
				// For multiple filters, check that result is within one of the filters' ranges
				found := false
				for _, filter := range tt.partitionFilters {
					if result.Begin >= filter.Begin && result.Begin < filter.Begin+filter.Count {
						found = true
						break
					}
				}
				require.True(t, found, "Partition ID %d is not within any of the expected ranges", result.Begin)
			}
		})
	}
}

func TestSumPartition(t *testing.T) {
	tests := []struct {
		name             string
		partitionFilters []*a.PartitionFilter
		expected         int
	}{
		{
			name:             "Empty filter list returns MaxPartitions",
			partitionFilters: []*a.PartitionFilter{},
			expected:         MaxPartitions,
		},
		{
			name: "Single filter with count 1",
			partitionFilters: []*a.PartitionFilter{
				{Begin: 0, Count: 1},
			},
			expected: 1,
		},
		{
			name: "Multiple filters",
			partitionFilters: []*a.PartitionFilter{
				{Begin: 0, Count: 10},
				{Begin: 100, Count: 20},
				{Begin: 200, Count: 30},
			},
			expected: 60, // 10 + 20 + 30
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sumPartition(tt.partitionFilters)
			assert.Equal(t, tt.expected, result)
		})
	}
}
