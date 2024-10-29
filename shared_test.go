package backup

import (
	"errors"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/stretchr/testify/assert"
)

func TestSplitNodes(t *testing.T) {
	tests := []struct {
		name           string
		nodes          []*a.Node
		numWorkers     int
		expectedCounts []int
		wantErr        error
	}{
		{
			name:           "numWorkers less than 1",
			nodes:          []*a.Node{{}, {}, {}},
			numWorkers:     0,
			expectedCounts: nil,
			wantErr:        errors.New("numWorkers is less than 1, cannot split nodes"),
		},
		{
			name:           "empty nodes list",
			nodes:          []*a.Node{},
			numWorkers:     3,
			expectedCounts: nil,
			wantErr:        errors.New("number of nodes is less than 1, cannot split nodes"),
		},
		{
			name:           "equal distribution",
			nodes:          []*a.Node{{}, {}, {}, {}},
			numWorkers:     2,
			expectedCounts: []int{2, 2},
			wantErr:        nil,
		},
		{
			name:           "more workers than nodes",
			nodes:          []*a.Node{{}, {}},
			numWorkers:     3,
			expectedCounts: []int{1, 1, 0},
			wantErr:        nil,
		},
		{
			name:           "single worker",
			nodes:          []*a.Node{{}, {}, {}},
			numWorkers:     1,
			expectedCounts: []int{3},
			wantErr:        nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := splitNodes(tt.nodes, tt.numWorkers)

			if tt.wantErr != nil {
				assert.EqualError(t, err, tt.wantErr.Error())
			} else {
				assert.NoError(t, err)

				var gotCounts []int
				for _, group := range got {
					gotCounts = append(gotCounts, len(group))
				}
				assert.Equal(t, tt.expectedCounts, gotCounts)
			}
		})
	}
}
