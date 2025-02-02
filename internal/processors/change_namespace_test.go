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
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
	"github.com/aws/smithy-go/ptr"
	"github.com/stretchr/testify/assert"
)

func TestChangeNamespaceProcessor(t *testing.T) {
	source := ptr.String("sourceNS")
	destination := ptr.String("destinationNS")

	key, _ := aerospike.NewKey(*source, "set", 1)
	invalidKey, _ := aerospike.NewKey("otherNs", "set", 1)

	tests := []struct {
		source       *string
		destination  *string
		initialToken *models.Token
		name         string
		wantErr      bool
	}{
		{
			name:        "nil restore Source",
			source:      nil,
			destination: destination,
			initialToken: models.NewRecordToken(&models.Record{
				Record: &aerospike.Record{
					Key: key,
				},
			}, 0, nil),
			wantErr: false,
		},
		{
			name:        "nil restore Destination",
			source:      source,
			destination: nil,
			initialToken: models.NewRecordToken(&models.Record{
				Record: &aerospike.Record{
					Key: key,
				},
			}, 0, nil),
			wantErr: false,
		},
		{
			name:         "non-record Token Type",
			source:       source,
			destination:  destination,
			initialToken: models.NewUDFToken(nil, 0),
			wantErr:      false,
		},
		{
			name:        "invalid source namespace",
			source:      source,
			destination: destination,
			initialToken: models.NewRecordToken(&models.Record{
				Record: &aerospike.Record{
					Key: invalidKey,
				},
			}, 0, nil),
			wantErr: true,
		},
		{
			name:        "valid process",
			source:      source,
			destination: destination,
			initialToken: models.NewRecordToken(&models.Record{
				Record: &aerospike.Record{
					Key: key,
				},
			}, 0, nil),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewChangeNamespace[*models.Token](tt.source, tt.destination)
			gotToken, err := p.Process(tt.initialToken)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.initialToken.Type == models.TokenTypeRecord && (tt.source != nil && tt.destination != nil) {
					assert.Equal(t, *tt.destination, gotToken.Record.Key.Namespace())
				}
			}
		})
	}
}
