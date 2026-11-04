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

package streamers

import (
	"testing"

	"github.com/aerospike/backup-go/pkg/server/segvalidator/segment"
)

const (
	// testAfterPartition is what a server manifest name holds past its
	// partition.
	testAfterPartition = "-7-0000181178352-db526a.json"
	// testPartitionName is testPartitionID as a manifest name spells it.
	testPartitionName = "935"
	testPartitionID   = 935
)

func TestPartitionOfManifest(t *testing.T) {
	t.Parallel()

	const testManifests = "519118324/ns/test/query-stream/manifest/"

	// Each case is read as sscanf(leaf, "%u-%u-%llu-%x.json") reads it.
	tests := []struct {
		name   string
		give   string
		wantID int
		wantOK bool
	}{
		{
			name:   "server name",
			give:   testManifests + testPartitionName + testAfterPartition,
			wantID: testPartitionID,
			wantOK: true,
		},
		{
			name:   "first partition",
			give:   testManifests + "0" + testAfterPartition,
			wantID: 0,
			wantOK: true,
		},
		{
			name:   "last partition",
			give:   testManifests + "4095" + testAfterPartition,
			wantID: segment.PartitionCount - 1,
			wantOK: true,
		},
		{
			name:   "leading zeros",
			give:   testManifests + "0935" + testAfterPartition,
			wantID: testPartitionID,
			wantOK: true,
		},
		{
			name:   "explicit sign",
			give:   testManifests + "+935" + testAfterPartition,
			wantID: testPartitionID,
			wantOK: true,
		},
		{
			name:   "white space before a number",
			give:   testManifests + " 935-7- 0000181178352-db526a.json",
			wantID: testPartitionID,
			wantOK: true,
		},
		{
			name:   "negative regime wraps around",
			give:   testManifests + "935--7-0000181178352-db526a.json",
			wantID: testPartitionID,
			wantOK: true,
		},
		{
			name:   "hexadecimal prefix of the uuid",
			give:   testManifests + "935-7-0000181178352-0xdb526a.json",
			wantID: testPartitionID,
			wantOK: true,
		},
		{
			name:   "nothing past the uuid is checked",
			give:   testManifests + "935-7-0000181178352-db526a-1.json",
			wantID: testPartitionID,
			wantOK: true,
		},
		{
			name:   "partition truncated to an unsigned int",
			give:   testManifests + "4294967296" + testAfterPartition,
			wantID: 0,
			wantOK: true,
		},
		{name: "partition out of range", give: testManifests + "4096" + testAfterPartition},
		{name: "negative partition wraps out of range", give: testManifests + "-1" + testAfterPartition},
		{
			name: "partition saturates out of range",
			give: testManifests + "99999999999999999999999" + testAfterPartition,
		},
		{name: "no uuid", give: testManifests + "935-7-0000181178352.json"},
		{name: "regime not a number", give: testManifests + "935-x-0000181178352-db526a.json"},
		{name: "timestamp not a number", give: testManifests + "935-7-x-db526a.json"},
		{name: "uuid not hexadecimal", give: testManifests + "935-7-0000181178352-xyz.json"},
		{name: "partition not a number", give: testManifests + "x" + testAfterPartition},
		{name: "no partition", give: testManifests + "manifest.json"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			id, ok := partitionOfManifest(tt.give)
			if id != tt.wantID || ok != tt.wantOK {
				t.Fatalf("partitionOfManifest() = (%d, %v), want (%d, %v)",
					id, ok, tt.wantID, tt.wantOK)
			}
		})
	}
}

// FuzzPartitionOfManifest feeds arbitrary names to the manifest name parser. A
// name comes from storage nobody vouches for, so no name may panic, and
// whatever is accepted must be a partition a namespace has.
func FuzzPartitionOfManifest(f *testing.F) {
	f.Add(testPartitionName + testAfterPartition)
	f.Add("+0x-")
	f.Add(" -99999999999999999999999--+-0x")
	f.Add("")

	f.Fuzz(func(t *testing.T, name string) {
		id, ok := partitionOfManifest(name)

		switch {
		case ok && (id < 0 || id >= segment.PartitionCount):
			t.Fatalf("partitionOfManifest(%q) = %d, outside the partitions", name, id)
		case !ok && id != 0:
			t.Fatalf("partitionOfManifest(%q) = (%d, false), want no partition", name, id)
		}
	})
}
