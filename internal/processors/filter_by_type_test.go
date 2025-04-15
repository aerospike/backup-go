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

package processors

import (
	"errors"
	"testing"

	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestNewFilterByType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		noRecords  bool
		noIndexes  bool
		noUdf      bool
		expectType any
	}{
		{
			name:       "No filters applied",
			noRecords:  false,
			noIndexes:  false,
			noUdf:      false,
			expectType: &noopProcessor[*models.Token]{},
		},
		{
			name:       "Only noRecords filter applied",
			noRecords:  true,
			noIndexes:  false,
			noUdf:      false,
			expectType: &filterByType[*models.Token]{},
		},
		{
			name:       "Only noIndexes filter applied",
			noRecords:  false,
			noIndexes:  true,
			noUdf:      false,
			expectType: &filterByType[*models.Token]{},
		},
		{
			name:       "Only noUdf filter applied",
			noRecords:  false,
			noIndexes:  false,
			noUdf:      true,
			expectType: &filterByType[*models.Token]{},
		},
		{
			name:       "All filters applied",
			noRecords:  true,
			noIndexes:  true,
			noUdf:      true,
			expectType: &filterByType[*models.Token]{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			processor := NewFilterByType[*models.Token](tt.noRecords, tt.noIndexes, tt.noUdf)
			assert.IsType(t, tt.expectType, processor)
		})
	}
}

func TestFilterByTypeProcess(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		filter       *filterByType[*models.Token]
		token        *models.Token
		expectError  bool
		errorMessage string
	}{
		{
			name: "Filter out record token with noRecords flag",
			filter: &filterByType[*models.Token]{
				noRecords: true,
				noIndexes: false,
				noUdf:     false,
			},
			token: &models.Token{
				Type: models.TokenTypeRecord,
			},
			expectError:  true,
			errorMessage: "record is filtered with no-records flag",
		},
		{
			name: "Allow record token with noRecords flag off",
			filter: &filterByType[*models.Token]{
				noRecords: false,
				noIndexes: false,
				noUdf:     false,
			},
			token:       &models.Token{Type: models.TokenTypeRecord},
			expectError: false,
		},
		{
			name: "Filter out sIndex token with noIndexes flag",
			filter: &filterByType[*models.Token]{
				noRecords: false,
				noIndexes: true,
				noUdf:     false,
			},
			token: &models.Token{
				Type: models.TokenTypeSIndex,
			},
			expectError:  true,
			errorMessage: "index is filtered with no-indexes flag",
		},
		{
			name: "Allow sIndex token with noIndexes flag off",
			filter: &filterByType[*models.Token]{
				noRecords: false,
				noIndexes: false,
				noUdf:     false,
			},
			token:       &models.Token{Type: models.TokenTypeSIndex},
			expectError: false,
		},
		{
			name: "Filter out UDF token with noUdf flag",
			filter: &filterByType[*models.Token]{
				noRecords: false,
				noIndexes: false,
				noUdf:     true,
			},
			token: &models.Token{
				Type: models.TokenTypeUDF,
			},
			expectError:  true,
			errorMessage: "udf is filtered with no-udf flag",
		},
		{
			name: "Allow UDF token with noUdf flag off",
			filter: &filterByType[*models.Token]{
				noRecords: false,
				noIndexes: false,
				noUdf:     false,
			},
			token:       &models.Token{Type: models.TokenTypeUDF},
			expectError: false,
		},
		{
			name: "No filtering applied",
			filter: &filterByType[*models.Token]{
				noRecords: false,
				noIndexes: false,
				noUdf:     false,
			},
			token:       &models.Token{Type: models.TokenTypeRecord},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := tt.filter.Process(tt.token)

			if tt.expectError {
				assert.Nil(t, result)
				assert.Error(t, err)
				assert.True(t, errors.Is(err, pipeline.ErrFilteredOut))
				assert.Contains(t, err.Error(), tt.errorMessage)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.token, result)
			}
		})
	}
}
