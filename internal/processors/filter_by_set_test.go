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
	"sync/atomic"
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
)

func TestSetFilter(t *testing.T) {
	type test struct {
		token            *models.Token
		setFilter        *filterBySet
		name             string
		shouldBeFiltered bool
	}

	setName := "set"
	key, _ := aerospike.NewKey("", setName, "")
	record := &models.Record{
		Record: &aerospike.Record{
			Key: key,
		},
	}
	tests := []test{
		{
			name: "Non-record token type",
			token: &models.Token{
				Type: models.TokenTypeSIndex,
			},
			setFilter: &filterBySet{
				setsToRestore: map[string]bool{
					"test": true,
				},
			},
			shouldBeFiltered: false,
		},
		{
			name: "No sets to restore",
			token: &models.Token{
				Type:   models.TokenTypeRecord,
				Record: record,
			},
			setFilter:        &filterBySet{setsToRestore: map[string]bool{}},
			shouldBeFiltered: false,
		},
		{
			name: "Token set not in restore list",
			token: &models.Token{
				Type:   models.TokenTypeRecord,
				Record: record,
			},
			setFilter: &filterBySet{
				setsToRestore: map[string]bool{
					"anotherSet": true,
				},
				skipped: &atomic.Uint64{},
			},
			shouldBeFiltered: true,
		},
		{
			name: "Token set in restore list",
			token: &models.Token{
				Type:   models.TokenTypeRecord,
				Record: record,
			},
			setFilter: &filterBySet{
				setsToRestore: map[string]bool{
					setName: true,
				},
			},
			shouldBeFiltered: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resToken, resErr := tc.setFilter.Process(tc.token)
			if tc.shouldBeFiltered {
				assert.Nil(t, resToken)
				assert.NotNil(t, resErr)
			} else {
				assert.Equal(t, tc.token, resToken)
				assert.Nil(t, resErr)
			}
		})
	}
}
