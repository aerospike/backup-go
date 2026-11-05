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
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEarliestAndLatestTime(t *testing.T) {
	t.Parallel()

	var zero time.Time

	early := testStartTime
	late := testFinishTime

	tests := []struct {
		name         string
		a, b         time.Time
		wantEarliest time.Time
		wantLatest   time.Time
	}{
		{
			name: "both zero",
		},
		{
			name:         "first zero",
			a:            zero,
			b:            early,
			wantEarliest: early,
			wantLatest:   early,
		},
		{
			name:         "second zero",
			a:            early,
			b:            zero,
			wantEarliest: early,
			wantLatest:   early,
		},
		{
			name:         "first earlier",
			a:            early,
			b:            late,
			wantEarliest: early,
			wantLatest:   late,
		},
		{
			name:         "first later",
			a:            late,
			b:            early,
			wantEarliest: early,
			wantLatest:   late,
		},
		{
			name:         "equal",
			a:            early,
			b:            early,
			wantEarliest: early,
			wantLatest:   early,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.wantEarliest, earliestTime(tt.a, tt.b))
			assert.Equal(t, tt.wantLatest, latestTime(tt.a, tt.b))
		})
	}
}

func TestResolveConsistency(t *testing.T) {
	t.Parallel()

	const testConsistencyUnknown Consistency = "partial"

	tests := []struct {
		name string
		give []Consistency
		want Consistency
	}{
		{name: "every node strict", give: []Consistency{ConsistencyStrict, ConsistencyStrict}, want: ConsistencyStrict},
		{name: "fuzzy on any node", give: []Consistency{ConsistencyStrict, ConsistencyFuzzy}, want: ConsistencyFuzzy},
		{name: "nodes reporting none are skipped", give: []Consistency{"", ConsistencyStrict}, want: ConsistencyStrict},
		{name: "unknown value is not strict", give: []Consistency{ConsistencyStrict, testConsistencyUnknown}, want: testConsistencyUnknown},
		{name: "fuzzy overrides unknown value", give: []Consistency{testConsistencyUnknown, ConsistencyFuzzy}, want: ConsistencyFuzzy},
		{name: "no node reports one", give: []Consistency{"", ""}, want: ""},
		{name: "no nodes", give: nil, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, ResolveConsistency(tt.give))
		})
	}
}

func TestParseOptionalUint64(t *testing.T) {
	t.Parallel()

	const testKey = "last-lap-ms"

	tests := []struct {
		name string
		give InfoMap
		want *uint64
	}{
		{name: "value", give: InfoMap{testKey: "250"}, want: new(uint64(250))},
		{name: "zero", give: InfoMap{testKey: "0"}, want: new(uint64(0))},
		{name: "not set", give: InfoMap{testKey: infoNoValue}, want: nil},
		{name: "missing", give: InfoMap{}, want: nil},
		{name: "unparsable", give: InfoMap{testKey: "soon"}, want: nil},
		{name: "negative", give: InfoMap{testKey: "-1"}, want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, parseOptionalUint64(tt.give, testKey))
		})
	}
}

func TestMaxOptional(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a    *uint64
		b    *uint64
		want *uint64
	}{
		{name: "both nil", want: nil},
		{name: "first nil", b: new(uint64(5)), want: new(uint64(5))},
		{name: "second nil", a: new(uint64(5)), want: new(uint64(5))},
		{name: "second larger", a: new(uint64(5)), b: new(uint64(9)), want: new(uint64(9))},
		{name: "first larger", a: new(uint64(9)), b: new(uint64(5)), want: new(uint64(9))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := maxOptional(tt.a, tt.b)

			assert.Equal(t, tt.want, got)

			if got != nil && tt.b != nil {
				assert.NotSame(t, tt.b, got, "the result must not alias the node value")
			}
		})
	}
}

func TestKnownName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		give string
		want bool
	}{
		{name: "namespace", give: "test", want: true},
		{name: "stub", give: infoNoName, want: false},
		{name: "no value", give: infoNoValue, want: false},
		{name: "empty", give: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, knownName(tt.give))
		})
	}
}
