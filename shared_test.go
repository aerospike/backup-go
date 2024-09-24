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
	"reflect"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v7"
)

func Test_splitPartitions(t *testing.T) {
	type args struct {
		startPartition int
		numPartitions  int
		numWorkers     int
	}
	tests := []struct {
		name    string
		want    []*a.PartitionFilter
		args    args
		wantErr bool
	}{
		{
			name: "Test positive splitPartitions",
			args: args{
				startPartition: 0,
				numPartitions:  10,
				numWorkers:     2,
			},
			want: []*a.PartitionFilter{
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
			name: "Test positive splitPartitions with start",
			args: args{
				startPartition: 5,
				numPartitions:  10,
				numWorkers:     2,
			},
			want: []*a.PartitionFilter{
				{
					Begin: 5,
					Count: 5,
				},
				{
					Begin: 10,
					Count: 5,
				},
			},
		},
		{
			name: "Test positive splitPartitions with start uneven",
			args: args{
				startPartition: 5,
				numPartitions:  20,
				numWorkers:     3,
			},
			want: []*a.PartitionFilter{
				{
					Begin: 5,
					Count: 6,
				},
				{
					Begin: 11,
					Count: 7,
				},
				{
					Begin: 18,
					Count: 7,
				},
			},
		},
		{
			name: "Test positive splitPartitions with 4096 partitions",
			args: args{
				startPartition: 0,
				numPartitions:  4096,
				numWorkers:     4,
			},
			want: []*a.PartitionFilter{
				{
					Begin: 0,
					Count: 1024,
				},
				{
					Begin: 1024,
					Count: 1024,
				},
				{
					Begin: 2048,
					Count: 1024,
				},
				{
					Begin: 3072,
					Count: 1024,
				},
			},
		},
		{
			name: "Test positive splitPartitions with 4096 partitions and 1 worker",
			args: args{
				startPartition: 0,
				numPartitions:  4096,
				numWorkers:     1,
			},
			want: []*a.PartitionFilter{
				{
					Begin: 0,
					Count: 4096,
				},
			},
		},
		{
			name: "Test negative splitPartitions with startPartition + numPartitions > partitions",
			args: args{
				startPartition: 200,
				numPartitions:  4096,
				numWorkers:     3,
			},
			wantErr: true,
		},
		{
			name: "Test negative splitPartitions with numWorkers < 1",
			args: args{
				startPartition: 200,
				numPartitions:  1,
				numWorkers:     0,
			},
			wantErr: true,
		},
		{
			name: "Test negative splitPartitions with numPartitions < 1",
			args: args{
				startPartition: 0,
				numPartitions:  0,
				numWorkers:     1,
			},
			wantErr: true,
		},
		{
			name: "Test negative splitPartitions with startPartitions < 0",
			args: args{
				startPartition: -1,
				numPartitions:  1,
				numWorkers:     1,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			partitionFilters := []*a.PartitionFilter{NewPartitionFilterByRange(tt.args.startPartition, tt.args.numPartitions)}
			got, err := splitPartitions(partitionFilters, tt.args.numWorkers)
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
