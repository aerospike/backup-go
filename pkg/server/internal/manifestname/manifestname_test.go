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

package manifestname

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseQuery(t *testing.T) {
	t.Parallel()

	const dir = "519118324/ns/test/query-stream/manifest/"

	// Each case is read as sscanf(leaf, "%u-%u-%llu-%x.json") reads it. The
	// partition rules are covered by the segvalidator's partitionOfManifest
	// tests; these cover the regime and timestamp.
	tests := []struct {
		name   string
		give   string
		want   Name
		wantOK bool
	}{
		{
			name:   "server name",
			give:   dir + "935-7-0000181178352-db526a.json",
			want:   Name{Partition: 935, Regime: 7, Timestamp: 181178352},
			wantOK: true,
		},
		{
			name:   "negative regime wraps around an unsigned int",
			give:   dir + "935--1-0000181178352-db526a.json",
			want:   Name{Partition: 935, Regime: math.MaxUint32, Timestamp: 181178352},
			wantOK: true,
		},
		{
			name:   "regime truncated to an unsigned int",
			give:   dir + "935-4294967297-5-db526a.json",
			want:   Name{Partition: 935, Regime: 1, Timestamp: 5},
			wantOK: true,
		},
		{
			name:   "timestamp saturates",
			give:   dir + "935-7-99999999999999999999999-db526a.json",
			want:   Name{Partition: 935, Regime: 7, Timestamp: math.MaxUint64},
			wantOK: true,
		},
		{name: "change stream name", give: dir + "7-0000181178352-db526a.json"},
		{name: "partition out of range", give: dir + "4096-7-0000181178352-db526a.json"},
		{name: "no uuid", give: dir + "935-7-0000181178352.json"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := ParseQuery(tt.give)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseChange(t *testing.T) {
	t.Parallel()

	const dir = "519118324/ns/test/change-stream/BB9D5F76B77F4A6/manifest/"

	// Each case is read as sscanf(leaf, "%u-%llu-") reads it, and the name must
	// end in .json.
	tests := []struct {
		name   string
		give   string
		want   Name
		wantOK bool
	}{
		{
			name:   "server name",
			give:   dir + "3-0000062031435-5e58e6.json",
			want:   Name{Regime: 3, Timestamp: 62031435},
			wantOK: true,
		},
		{
			name:   "nothing past the timestamp is checked",
			give:   dir + "3-0000062031435.json",
			want:   Name{Regime: 3, Timestamp: 62031435},
			wantOK: true,
		},
		{
			name:   "white space before a number",
			give:   dir + " 3- 0000062031435-5e58e6.json",
			want:   Name{Regime: 3, Timestamp: 62031435},
			wantOK: true,
		},
		{name: "not json", give: dir + "3-0000062031435-5e58e6.seg"},
		{name: "no timestamp", give: dir + "3-x-5e58e6.json"},
		{name: "regime not a number", give: dir + "x-0000062031435-5e58e6.json"},
		{name: "no separator", give: dir + "3.json"},
		{name: "empty", give: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := ParseChange(tt.give)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestName_Newer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		n     Name
		other Name
		want  bool
	}{
		{name: "higher regime", n: Name{Regime: 2, Timestamp: 1}, other: Name{Regime: 1, Timestamp: 9}, want: true},
		{name: "lower regime", n: Name{Regime: 1, Timestamp: 9}, other: Name{Regime: 2, Timestamp: 1}},
		{name: "same regime, later", n: Name{Regime: 1, Timestamp: 2}, other: Name{Regime: 1, Timestamp: 1}, want: true},
		{name: "same regime, earlier", n: Name{Regime: 1, Timestamp: 1}, other: Name{Regime: 1, Timestamp: 2}},
		{name: "equal", n: Name{Regime: 1, Timestamp: 1}, other: Name{Regime: 1, Timestamp: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.n.Newer(tt.other))
		})
	}
}

// FuzzParse feeds arbitrary names to both parsers. A name comes from storage
// nobody vouches for, so no name may panic, a rejected name yields no fields,
// and an accepted query name has a partition a namespace has.
func FuzzParse(f *testing.F) {
	f.Add("935-7-0000181178352-db526a.json")
	f.Add("3-0000062031435-5e58e6.json")
	f.Add(" -99999999999999999999999--+-0x")
	f.Add("")

	f.Fuzz(func(t *testing.T, leaf string) {
		if name, ok := ParseQuery(leaf); ok && (name.Partition < 0 || name.Partition >= 4096) ||
			!ok && name != (Name{}) {
			t.Fatalf("ParseQuery(%q) = (%+v, %v)", leaf, name, ok)
		}

		if name, ok := ParseChange(leaf); ok && name.Partition != 0 || !ok && name != (Name{}) {
			t.Fatalf("ParseChange(%q) = (%+v, %v)", leaf, name, ok)
		}
	})
}
