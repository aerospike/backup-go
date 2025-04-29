package backup

import (
	"errors"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/mocks"
	"github.com/stretchr/testify/assert"
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
			expected:    1000, // 1 + 10 + 100 + 913 = 1024 (of 4096)
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
			mockInfoClient := mocks.NewMockinfoGetter(t)
			mockInfoClient.On("GetRecordCount", tt.namespace, []string{"set1"}).Return(tt.recordCount, tt.infoError)

			handler := &backupRecordsHandler{
				config: &ConfigBackup{
					Namespace:        tt.namespace,
					SetList:          []string{"set1"},
					PartitionFilters: tt.partFilters,
				},
			}

			result, err := handler.countUsingInfoClient(mockInfoClient)

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
