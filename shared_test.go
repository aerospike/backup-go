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

package backup

import (
	"reflect"
	"testing"
)

func Test_splitPartitions(t *testing.T) {
	type args struct {
		numPartitions int
		numWorkers    int
	}
	tests := []struct {
		name    string
		args    args
		want    []PartitionRange
		wantErr bool
	}{
		{
			name: "Test positive splitPartitions",
			args: args{
				numPartitions: 10,
				numWorkers:    2,
			},
			want: []PartitionRange{
				{
					Begin: 0,
					Count: 5,
				},
				{
					Begin: 5,
					Count: 5,
				},
			},
		},
		{
			name: "Test positive splitPartitions with uneven partitions",
			args: args{
				numPartitions: 11,
				numWorkers:    3,
			},
			want: []PartitionRange{
				{
					Begin: 0,
					Count: 3,
				},
				{
					Begin: 3,
					Count: 4,
				},
				{
					Begin: 7,
					Count: 4,
				},
			},
		},
		{
			name: "Test positive splitPartitions with 4096 partitions",
			args: args{
				numPartitions: 4096,
				numWorkers:    5,
			},
			want: []PartitionRange{
				{
					Begin: 0,
					Count: 819,
				},
				{
					Begin: 819,
					Count: 819,
				},
				{
					Begin: 1638,
					Count: 819,
				},
				{
					Begin: 2457,
					Count: 819,
				},
				{
					Begin: 3276,
					Count: 820,
				},
			},
		},
		{
			name: "Test positive splitPartitions with 1 numPartitions",
			args: args{
				numPartitions: 1,
				numWorkers:    1,
			},
			want: []PartitionRange{
				{
					Begin: 0,
					Count: 1,
				},
			},
		},
		{
			name: "Test negative splitPartitions with 0 numPartitions",
			args: args{
				numPartitions: 0,
				numWorkers:    2,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Test negative splitPartitions with 0 numWorkers",
			args: args{
				numPartitions: 10,
				numWorkers:    0,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := splitPartitions(tt.args.numPartitions, tt.args.numWorkers)
			if (err != nil) != tt.wantErr {
				t.Errorf("splitPartitions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("splitPartitions() = %v, want %v", got, tt.want)
			}
		})
	}
}
