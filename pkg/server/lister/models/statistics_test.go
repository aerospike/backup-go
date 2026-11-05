// Copyright 2024-2026 Aerospike, Inc.
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

package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatistics_Add(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		s     Statistics
		other Statistics
		want  Statistics
	}{
		{
			name: "every count is added",
			s:    Statistics{RecordCount: 1, IncrRecordCount: 2, ChangeRecordCount: 3, ByteCount: 10, FileCount: 2},
			other: Statistics{
				RecordCount: 3, IncrRecordCount: 4, ChangeRecordCount: 5, ByteCount: 30, FileCount: 4,
			},
			want: Statistics{
				RecordCount: 4, IncrRecordCount: 6, ChangeRecordCount: 8, ByteCount: 40, FileCount: 6,
			},
		},
		{
			name:  "adding zero keeps the counts",
			s:     Statistics{RecordCount: 1, ByteCount: 10, FileCount: 2},
			other: Statistics{},
			want:  Statistics{RecordCount: 1, ByteCount: 10, FileCount: 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.s.Add(tt.other)
			assert.Equal(t, tt.want, tt.s)
		})
	}
}
