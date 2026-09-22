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
	"regexp"
	"sort"
	"sync"
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

	// testConcurrentIDsPerGoroutine keeps every concurrent case well inside
	// jobIDSaltSpace, so that a duplicate can only come from a broken counter and
	// never from the salt legitimately wrapping around.
	testConcurrentIDsPerGoroutine = 1000
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

// TestNewJobID_ConcurrentUniqueness covers the same guarantee under load: the salt
// counter is shared package state, so ids must stay unique when several goroutines
// ask for one at the same instant. Run it with -race to also catch unsynchronized
// access to the counter.
func TestNewJobID_ConcurrentUniqueness(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 16, 14, 20, 35, 0, time.UTC)

	tests := []struct {
		name       string
		goroutines int
		perRoutine int
	}{
		{
			name:       "one id per goroutine",
			goroutines: 128,
			perRoutine: 1,
		},
		{
			name:       "two goroutines in a tight loop",
			goroutines: 2,
			perRoutine: testConcurrentIDsPerGoroutine,
		},
		{
			name:       "many goroutines in a tight loop",
			goroutines: 32,
			perRoutine: testConcurrentIDsPerGoroutine,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var (
				wg sync.WaitGroup
				mu sync.Mutex
			)

			want := tt.goroutines * tt.perRoutine
			seen := make(map[string]struct{}, want)

			wg.Add(tt.goroutines)

			for range tt.goroutines {
				go func() {
					defer wg.Done()

					// Collect locally and merge once, so the lock does not serialize
					// the generator the test is meant to hammer.
					ids := make([]string, 0, tt.perRoutine)
					for range tt.perRoutine {
						ids = append(ids, newJobIDForTime(now))
					}

					mu.Lock()
					defer mu.Unlock()

					for _, id := range ids {
						seen[id] = struct{}{}
					}
				}()
			}

			wg.Wait()

			require.Len(t, seen, want, "duplicate ids generated concurrently")
		})
	}
}

// TestQuoteCharClass guards the matcher against a change of base36Digits: every
// character must end up literal inside the class, with no range and no POSIX class
// forming by accident.
func TestQuoteCharClass(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    string
		rejects []string
	}{
		{
			name:    "salt alphabet needs no escaping",
			in:      base36Digits,
			want:    base36Digits,
			rejects: []string{"-", "A", "_"},
		},
		{
			name:    "range mark is escaped",
			in:      "a-z",
			want:    `a\-z`,
			rejects: []string{"m"},
		},
		{
			name:    "negation mark is escaped",
			in:      "^a",
			want:    `\^a`,
			rejects: []string{"b"},
		},
		{
			name:    "closing bracket is escaped",
			in:      "a]b",
			want:    `a\]b`,
			rejects: []string{"c"},
		},
		{
			name:    "backslash is escaped",
			in:      `a\b`,
			want:    `a\\b`,
			rejects: []string{"c"},
		},
		{
			name:    "posix class opener is escaped",
			in:      "[:digit:]",
			want:    `\[:digit:\]`,
			rejects: []string{"5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := quoteCharClass(tt.in)
			require.Equal(t, tt.want, got)

			re := regexp.MustCompile("^[" + got + "]$")

			for _, c := range tt.in {
				require.True(t, re.MatchString(string(c)), "class does not match %q", c)
			}

			for _, r := range tt.rejects {
				require.False(t, re.MatchString(r), "class unexpectedly matches %q", r)
			}
		})
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
