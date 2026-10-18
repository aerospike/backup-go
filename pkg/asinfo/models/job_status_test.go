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

func TestFirstSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a, b string
		want string
	}{
		{
			name: "both unset",
		},
		{
			name: "first set",
			a:    testErrorReason,
			b:    testAbortReason,
			want: testErrorReason,
		},
		{
			name: "only second set",
			b:    testAbortReason,
			want: testAbortReason,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, firstSet(tt.a, tt.b))
		})
	}
}
