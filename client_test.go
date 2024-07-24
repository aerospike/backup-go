// Copyright 2024-2024 Aerospike, Inc.
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

//go:build test
// +build test

package backup

import (
	"testing"
)

func TestPartitionRange_validate(t *testing.T) {
	type fields struct {
		Begin int
		Count int
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "Test positive PartitionRange validate",
			fields: fields{
				Begin: 0,
				Count: 5,
			},
			wantErr: false,
		},
		{
			name: "Test negative PartitionRange validate count 0",
			fields: fields{
				Begin: 5,
				Count: 0,
			},
			wantErr: true,
		},
		{
			name: "Test negative PartitionRange validate begin -1",
			fields: fields{
				Begin: -1,
				Count: 5,
			},
			wantErr: true,
		},
		{
			name: "Test negative PartitionRange validate count -1",
			fields: fields{
				Begin: 5,
				Count: -1,
			},
			wantErr: true,
		},
		{
			name: "Test negative PartitionRange validate total partitions greater than 4096",
			fields: fields{
				Begin: 4000,
				Count: 1000,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := PartitionRange{
				Begin: tt.fields.Begin,
				Count: tt.fields.Count,
			}
			if err := p.validate(); (err != nil) != tt.wantErr {
				t.Errorf("PartitionRange.validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
