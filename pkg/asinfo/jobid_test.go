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

package asinfo

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testJobIDPattern   = `^[0-9]{6}T[0-9]{6}-[0-9a-z]{4}$`
	testJobID          = "260316T142035-k3f9"
	testTimeZoneIsrael = "Asia/Jerusalem"
	testSameSecondIDs  = 10000

	testGeneratedIDsPerMoment = 100
)

func TestNewJobID_TimestampPart(t *testing.T) {
	t.Parallel()

	israel, err := time.LoadLocation(testTimeZoneIsrael)
	require.NoError(t, err)

	tests := []struct {
		name     string
		now      time.Time
		wantTime string
	}{
		{
			name:     "utc time is formatted as is",
			now:      time.Date(2026, 3, 16, 14, 20, 35, 0, time.UTC),
			wantTime: "260316T142035",
		},
		{
			name:     "local time is converted to utc",
			now:      time.Date(2026, 3, 16, 16, 20, 35, 0, israel),
			wantTime: "260316T142035",
		},
		{
			name:     "single digit fields are zero padded",
			now:      time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
			wantTime: "260102T030405",
		},
		{
			name:     "sub second precision is dropped",
			now:      time.Date(2026, 3, 16, 14, 20, 35, 999999999, time.UTC),
			wantTime: "260316T142035",
		},
		{
			name:     "end of year",
			now:      time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC),
			wantTime: "261231T235959",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := newJobIDForTime(tt.now)

			require.Equal(t, tt.wantTime, got[:len(jobIDTimeLayout)])
			require.Len(t, got, jobIDLen)
			require.Regexp(t, testJobIDPattern, got)
		})
	}
}

func TestNewJobID_ServerConstraints(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 16, 14, 20, 35, 0, time.UTC)

	tests := []struct {
		name      string
		assertion func(t *testing.T, id string)
	}{
		{
			name: "fits the server cap",
			assertion: func(t *testing.T, id string) {
				t.Helper()
				require.LessOrEqual(t, len(id), jobIDMaxLen)
			},
		},
		{
			name: "contains no info protocol delimiters",
			assertion: func(t *testing.T, id string) {
				t.Helper()
				require.NotRegexp(t, `[|;:=,]`, id)
			},
		},
		{
			name: "is a safe s3 key segment",
			assertion: func(t *testing.T, id string) {
				t.Helper()
				require.Regexp(t, `^[0-9A-Za-z-]+$`, id)
				require.NotRegexp(t, `^-|-$`, id)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.assertion(t, newJobIDForTime(now))
		})
	}
}

// TestNewJobID_SameSecondUniqueness covers the failure this format exists to fix:
// two jobs started within the same second must not share an id.
func TestNewJobID_SameSecondUniqueness(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 16, 14, 20, 35, 0, time.UTC)
	seen := make(map[string]struct{}, testSameSecondIDs)

	for range testSameSecondIDs {
		id := newJobIDForTime(now)

		_, duplicate := seen[id]
		require.False(t, duplicate, "duplicate id generated: %s", id)

		seen[id] = struct{}{}
	}
}

func TestIsJobID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want bool
	}{
		{
			name: "generated id form",
			in:   testJobID,
			want: true,
		},
		{
			name: "salt of digits only",
			in:   "260316T142035-0000",
			want: true,
		},
		{
			name: "salt of letters only",
			in:   "260316T142035-zzzz",
			want: true,
		},
		{
			name: "implausible but well formed timestamp",
			in:   "999999T999999-k3f9",
			want: true,
		},
		{
			name: "empty",
			in:   "",
			want: false,
		},
		{
			name: "citrusleaf timestamp folder",
			in:   "300000000",
			want: false,
		},
		{
			name: "no salt",
			in:   "260316T142035",
			want: false,
		},
		{
			name: "missing separator",
			in:   "260316T142035k3f9",
			want: false,
		},
		{
			name: "short salt",
			in:   "260316T142035-k3f",
			want: false,
		},
		{
			name: "long salt",
			in:   "260316T142035-k3f9a",
			want: false,
		},
		{
			name: "uppercase salt",
			in:   "260316T142035-K3F9",
			want: false,
		},
		{
			name: "lowercase time separator",
			in:   "260316t142035-k3f9",
			want: false,
		},
		{
			name: "short timestamp",
			in:   "26036T142035-k3f9",
			want: false,
		},
		{
			name: "leading whitespace",
			in:   " " + testJobID,
			want: false,
		},
		{
			name: "trailing newline",
			in:   testJobID + "\n",
			want: false,
		},
		{
			name: "storage path instead of a folder name",
			in:   "backups/" + testJobID,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, IsJobID(tt.in))
		})
	}
}

// TestIsJobID_AcceptsGeneratedIDs keeps the matcher and the generator from drifting
// apart: every id this package emits must be recognized as one.
func TestIsJobID_AcceptsGeneratedIDs(t *testing.T) {
	t.Parallel()

	moments := []time.Time{
		time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		time.Date(2026, 3, 16, 14, 20, 35, 0, time.UTC),
		time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC),
	}

	for _, m := range moments {
		// Several ids per moment, so that salts of different natural widths are covered.
		for range testGeneratedIDsPerMoment {
			id := newJobIDForTime(m)
			require.True(t, IsJobID(id), "generated id not recognized: %s", id)
		}
	}
}

// TestNewJobID_LexicalOrderMatchesTime guards the fixed field widths: listing tools
// sort ids as plain strings and expect chronological order.
func TestNewJobID_LexicalOrderMatchesTime(t *testing.T) {
	t.Parallel()

	moments := []time.Time{
		time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC),
		time.Date(2026, 1, 2, 3, 5, 0, 0, time.UTC),
		time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	ids := make([]string, 0, len(moments))
	for _, m := range moments {
		ids = append(ids, newJobIDForTime(m))
	}

	sorted := make([]string, len(ids))
	copy(sorted, ids)
	sort.Strings(sorted)

	require.Equal(t, ids, sorted)
}
