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
	"github.com/stretchr/testify/require"
)

// Shared by the restore status tests below.
const (
	testBackupIDFull  = "full-20261018"
	testBackupIDIncr1 = "incr-20261019"
	testBackupIDIncr2 = "incr-20261020"
	testClusterKey    = "4f1c2a9b7d10"
	testThreads       = "8"
	testFalse         = "false"

	testKindCold    = string(RestoreKindCold)
	testStateActive = string(RestoreStateActive)
	testNsStateNone = string(NamespaceStateNone)

	testNsStateUnknown    NamespaceState = "DRAINING"
	testClearStateUnknown ClearState     = "verifying"
)

func TestNewResponseRestoreState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input []InfoMap
		want  *ResponseRestoreState
	}{
		{
			name: "cold job with every field reported",
			input: []InfoMap{
				{
					fieldJobID:                  testJobID,
					fieldNamespace:              testNamespace,
					fieldKind:                   testKindCold,
					fieldState:                  testStateActive,
					fieldRestoreRPS:             "1000",
					fieldRestoreWorkerThreads:   testThreads,
					fieldRestoreDownloadThreads: "16",
					fieldNsState:                string(NamespaceStateRestoring),
					fieldStartTime:              testStartTimeStr,
					fieldFinishTime:             infoNoValue,
					fieldJoiner:                 testFalse,
					fieldPlaceholder:            "true",
					fieldPidsInScope:            "4096",
					fieldMissingLayoutNodes:     "1",
					fieldNDoneNodes:             "2",
					fieldBackupID:               testBackupIDIncr2,
					fieldBackupEntry:            "2",
					fieldBackupEntries:          "3",
					fieldExtraTTL:               "3600",
					fieldTTLExtended:            "70",
					fieldTTLClamped:             "5",
					fieldClusterKey:             testClusterKey,
					fieldDone:                   testFalse,
					fieldClear:                  string(ClearStateNone),
					fieldClearZeroedBytes:       "4294967296",
					fieldClearMs:                "8421",
				},
			},
			want: &ResponseRestoreState{
				StartTime:              testStartTime,
				JobID:                  testJobID,
				Namespace:              testNamespace,
				Kind:                   RestoreKindCold,
				State:                  RestoreStateActive,
				NsState:                NamespaceStateRestoring,
				RestoreRPS:             1000,
				RestoreWorkerThreads:   8,
				RestoreDownloadThreads: 16,
				BackupID:               testBackupIDIncr2,
				BackupEntry:            2,
				BackupEntries:          3,
				ExtraTTL:               3600,
				TTLExtended:            70,
				TTLClamped:             5,
				Placeholder:            true,
				PidsInScope:            4096,
				MissingLayoutNodes:     1,
				NDoneNodes:             2,
				ClusterKey:             testClusterKey,
				Clear:                  ClearStateNone,
				ClearZeroedBytes:       4294967296,
				ClearMs:                8421,
			},
		},
		{
			name: "fuzzy job with every field reported",
			input: []InfoMap{
				{
					fieldJobID:                  testJobID,
					fieldNamespace:              testNamespace,
					fieldKind:                   string(RestoreKindFuzzy),
					fieldState:                  string(RestoreStateAborted),
					fieldRestoreRPS:             "0",
					fieldRestoreWorkerThreads:   testThreads,
					fieldRestoreDownloadThreads: testThreads,
					fieldNsState:                testNsStateNone,
					fieldStartTime:              testStartTimeStr,
					fieldFinishTime:             testFinishTimeStr,
					fieldApplied:                "900",
					fieldIgnored:                "3",
					fieldFailed:                 "1",
					fieldOutstanding:            "7",
					fieldSkipped:                "20",
					fieldHandedOff:              "4",
					fieldCovered:                "1000",
					fieldClusterWide:            "3000",
					fieldRead:                   "935",
					fieldBackupID:               testBackupIDIncr1,
					fieldBackupEntry:            "1",
					fieldBackupEntries:          "2",
					fieldDeleted:                "11",
					fieldDeleteAbsent:           "6",
					fieldErrorReason:            testErrorReason,
					fieldAbortOrigin:            string(AbortOriginServer),
				},
			},
			want: &ResponseRestoreState{
				StartTime:              testStartTime,
				FinishTime:             testFinishTime,
				JobID:                  testJobID,
				Namespace:              testNamespace,
				Kind:                   RestoreKindFuzzy,
				State:                  RestoreStateAborted,
				NsState:                NamespaceStateNone,
				ErrorReason:            testErrorReason,
				AbortOrigin:            AbortOriginServer,
				RestoreWorkerThreads:   8,
				RestoreDownloadThreads: 8,
				BackupID:               testBackupIDIncr1,
				BackupEntry:            1,
				BackupEntries:          2,
				Applied:                900,
				Ignored:                3,
				Failed:                 1,
				Outstanding:            7,
				Skipped:                20,
				HandedOff:              4,
				Covered:                1000,
				ClusterWide:            3000,
				Read:                   935,
				Deleted:                11,
				DeleteAbsent:           6,
			},
		},
		{
			name: "node that does not host the namespace",
			input: []InfoMap{
				{
					fieldJobID: testJobID, fieldNamespace: testNamespace, fieldKind: testKindCold,
					fieldState: testStateActive, fieldNsState: infoNoValue,
				},
			},
			want: &ResponseRestoreState{
				JobID:     testJobID,
				Namespace: testNamespace,
				Kind:      RestoreKindCold,
				State:     RestoreStateActive,
				NsState:   NamespaceStateNotHosted,
			},
		},
		{
			name: "unparsable values leave the fields at zero",
			input: []InfoMap{
				{fieldJobID: testJobID, fieldState: testStateActive, fieldApplied: "many", fieldDone: "maybe", fieldCovered: "12"},
			},
			want: &ResponseRestoreState{
				JobID:   testJobID,
				State:   RestoreStateActive,
				Covered: 12,
			},
		},
		{
			name:  "namespace gate without a job",
			input: []InfoMap{{fieldNamespace: testNamespace, fieldNsState: testNsStateNone}},
			want:  nil,
		},
		{
			name:  "empty response",
			input: nil,
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, NewResponseRestoreState(tt.input))
		})
	}
}

func TestResolveNamespaceState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		states []NamespaceState
		want   NamespaceState
	}{
		{
			name:   "ready only when every node is ready",
			states: []NamespaceState{NamespaceStateReady, NamespaceStateReady},
			want:   NamespaceStateReady,
		},
		{
			name:   "preparing node holds the cluster back",
			states: []NamespaceState{NamespaceStateReady, NamespaceStatePreparing},
			want:   NamespaceStatePreparing,
		},
		{
			name:   "failed overrides all",
			states: []NamespaceState{NamespaceStateRestoring, NamespaceStateFailed, NamespaceStatePreparing},
			want:   NamespaceStateFailed,
		},
		{
			name:   "restoring overrides preparing",
			states: []NamespaceState{NamespaceStatePreparing, NamespaceStateRestoring},
			want:   NamespaceStateRestoring,
		},
		{
			name:   "opening until every node is released",
			states: []NamespaceState{NamespaceStateNone, NamespaceStateOpening},
			want:   NamespaceStateOpening,
		},
		{
			name:   "none overrides ready",
			states: []NamespaceState{NamespaceStateReady, NamespaceStateNone},
			want:   NamespaceStateNone,
		},
		{
			name:   "nodes not hosting the namespace are skipped",
			states: []NamespaceState{NamespaceStateNotHosted, NamespaceStateReady, ""},
			want:   NamespaceStateReady,
		},
		{
			name:   "unknown state overrides every known state but failed",
			states: []NamespaceState{NamespaceStateReady, testNsStateUnknown, NamespaceStateRestoring},
			want:   testNsStateUnknown,
		},
		{
			name:   "failed overrides an unknown state",
			states: []NamespaceState{testNsStateUnknown, NamespaceStateFailed},
			want:   NamespaceStateFailed,
		},
		{
			name:   "no node hosts the namespace",
			states: []NamespaceState{NamespaceStateNotHosted},
			want:   "",
		},
		{
			name:   "no states at all",
			states: nil,
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, ResolveNamespaceState(tt.states))
		})
	}
}

func TestResolveClearState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		states []ClearState
		want   ClearState
	}{
		{
			name:   "none only when no node needs or runs a clear",
			states: []ClearState{ClearStateNone, ClearStateNone},
			want:   ClearStateNone,
		},
		{
			name:   "clearing overrides none",
			states: []ClearState{ClearStateNone, ClearStateClearing},
			want:   ClearStateClearing,
		},
		{
			name:   "required overrides all",
			states: []ClearState{ClearStateClearing, testClearStateUnknown, ClearStateRequired, ClearStateNone},
			want:   ClearStateRequired,
		},
		{
			name:   "unknown state overrides every known state but required",
			states: []ClearState{ClearStateNone, testClearStateUnknown, ClearStateClearing},
			want:   testClearStateUnknown,
		},
		{
			name:   "nodes reporting no state are skipped",
			states: []ClearState{"", ClearStateClearing},
			want:   ClearStateClearing,
		},
		{
			name:   "no node reports a state",
			states: []ClearState{""},
			want:   "",
		},
		{
			name:   "no states at all",
			states: nil,
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, ResolveClearState(tt.states))
		})
	}
}

func TestResolveRestoreState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		states []RestoreState
		want   RestoreState
	}{
		{
			name:   "failed overrides all",
			states: []RestoreState{RestoreStateAborted, RestoreStateFailed, RestoreStateAborting},
			want:   RestoreStateFailed,
		},
		{
			name:   "aborting overrides aborted",
			states: []RestoreState{RestoreStateAborted, RestoreStateAborting},
			want:   RestoreStateAborting,
		},
		{
			name:   "aborted overrides active",
			states: []RestoreState{RestoreStateActive, RestoreStateAborted},
			want:   RestoreStateAborted,
		},
		{
			name:   "active until every node is complete",
			states: []RestoreState{RestoreStateComplete, RestoreStateActive},
			want:   RestoreStateActive,
		},
		{
			name:   "complete on every node",
			states: []RestoreState{RestoreStateComplete, RestoreStateComplete},
			want:   RestoreStateComplete,
		},
		{
			name:   "complete mixed with an unknown state",
			states: []RestoreState{RestoreStateComplete, RestoreStateUnknown},
			want:   RestoreStateUnknown,
		},
		{
			name:   "no states at all",
			states: nil,
			want:   RestoreStateUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, ResolveRestoreState(tt.states))
		})
	}
}

func TestMergeResponseRestoreStates(t *testing.T) {
	t.Parallel()

	startLate := testStartTime.Add(time.Second)
	finishEarly := testFinishTime.Add(-time.Second)

	tests := []struct {
		name     string
		statuses []*ResponseRestoreState
		want     *ResponseRestoreState
	}{
		{
			name: "cold job takes the node with the highest backup entry",
			statuses: []*ResponseRestoreState{
				{
					StartTime:              startLate,
					FinishTime:             testFinishTime,
					JobID:                  testJobID,
					Namespace:              testNamespace,
					Kind:                   RestoreKindCold,
					State:                  RestoreStateComplete,
					NsState:                NamespaceStateOpening,
					RestoreRPS:             100,
					RestoreWorkerThreads:   8,
					RestoreDownloadThreads: 8,
					BackupID:               testBackupIDFull,
					BackupEntry:            0,
					BackupEntries:          3,
					ExtraTTL:               60,
					TTLExtended:            10,
					TTLClamped:             1,
					PidsInScope:            4096,
					NDoneNodes:             2,
					ClusterKey:             testClusterKey,
					Done:                   true,
					Clear:                  ClearStateNone,
					ClearZeroedBytes:       1000,
					ClearMs:                900,
				},
				nil,
				{
					StartTime:              testStartTime,
					FinishTime:             finishEarly,
					JobID:                  testJobID,
					Namespace:              testNamespace,
					Kind:                   RestoreKindCold,
					State:                  RestoreStateActive,
					NsState:                NamespaceStateRestoring,
					RestoreRPS:             100,
					RestoreWorkerThreads:   8,
					RestoreDownloadThreads: 8,
					BackupID:               testBackupIDIncr1,
					BackupEntry:            1,
					BackupEntries:          3,
					ExtraTTL:               60,
					TTLExtended:            20,
					TTLClamped:             2,
					PidsInScope:            4096,
					MissingLayoutNodes:     1,
					NDoneNodes:             1,
					Joiner:                 true,
					Clear:                  ClearStateClearing,
					ClearZeroedBytes:       500,
					ClearMs:                1200,
				},
			},
			want: &ResponseRestoreState{
				StartTime:              testStartTime,
				FinishTime:             testFinishTime,
				JobID:                  testJobID,
				Namespace:              testNamespace,
				Kind:                   RestoreKindCold,
				State:                  RestoreStateActive,
				NsState:                NamespaceStateRestoring,
				RestoreRPS:             100,
				RestoreWorkerThreads:   8,
				RestoreDownloadThreads: 8,
				BackupID:               testBackupIDIncr1,
				BackupEntry:            1,
				BackupEntries:          3,
				ExtraTTL:               60,
				TTLExtended:            30,
				TTLClamped:             3,
				PidsInScope:            4096,
				MissingLayoutNodes:     1,
				NDoneNodes:             2,
				ClusterKey:             testClusterKey,
				Joiner:                 true,
				Done:                   true,
				Clear:                  ClearStateClearing,
				ClearZeroedBytes:       1500,
				ClearMs:                1200,
			},
		},
		{
			name: "fuzzy job takes the node with the lowest backup entry and sums counters",
			statuses: []*ResponseRestoreState{
				{
					JobID:         testJobID,
					Namespace:     testNamespace,
					Kind:          RestoreKindFuzzy,
					State:         RestoreStateFailed,
					NsState:       NamespaceStateNone,
					ErrorReason:   testErrorReason,
					BackupID:      testBackupIDIncr2,
					BackupEntry:   2,
					BackupEntries: 3,
					Applied:       10,
					Ignored:       1,
					Failed:        1,
					Outstanding:   2,
					Skipped:       3,
					HandedOff:     4,
					Covered:       100,
					ClusterWide:   250,
					Read:          20,
					Deleted:       5,
					DeleteAbsent:  1,
				},
				{
					JobID:         testJobID,
					Namespace:     testNamespace,
					Kind:          RestoreKindFuzzy,
					State:         RestoreStateAborting,
					NsState:       NamespaceStateNone,
					ErrorReason:   testAbortReason,
					AbortOrigin:   AbortOriginOperator,
					BackupID:      testBackupIDIncr1,
					BackupEntry:   1,
					BackupEntries: 3,
					Applied:       30,
					Ignored:       2,
					Failed:        0,
					Outstanding:   1,
					Skipped:       6,
					HandedOff:     1,
					Covered:       150,
					ClusterWide:   300,
					Read:          40,
					Deleted:       2,
					DeleteAbsent:  2,
				},
			},
			want: &ResponseRestoreState{
				JobID:         testJobID,
				Namespace:     testNamespace,
				Kind:          RestoreKindFuzzy,
				State:         RestoreStateFailed,
				NsState:       NamespaceStateNone,
				ErrorReason:   testErrorReason,
				AbortOrigin:   AbortOriginOperator,
				BackupID:      testBackupIDIncr1,
				BackupEntry:   1,
				BackupEntries: 3,
				Applied:       40,
				Ignored:       3,
				Failed:        1,
				Outstanding:   3,
				Skipped:       9,
				HandedOff:     5,
				Covered:       250,
				ClusterWide:   300,
				Read:          60,
				Deleted:       7,
				DeleteAbsent:  3,
			},
		},
		{
			name: "single backup id reports no entry",
			statuses: []*ResponseRestoreState{
				{
					JobID:   testJobID,
					Kind:    RestoreKindCold,
					State:   RestoreStateActive,
					NsState: NamespaceStateReady,
				},
				{
					JobID:   testJobID,
					Kind:    RestoreKindCold,
					State:   RestoreStateActive,
					NsState: NamespaceStateNotHosted,
				},
			},
			want: &ResponseRestoreState{
				JobID:   testJobID,
				Kind:    RestoreKindCold,
				State:   RestoreStateActive,
				NsState: NamespaceStateReady,
			},
		},
		{
			name:     "only nil statuses",
			statuses: []*ResponseRestoreState{nil, nil},
			want:     nil,
		},
		{
			name:     "no statuses",
			statuses: nil,
			want:     nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := MergeResponseRestoreStates(tt.statuses)
			if tt.want == nil {
				require.Nil(t, got)

				return
			}

			require.NotNil(t, got)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestResponseRestoreState_IsStarted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		give ResponseRestoreState
		want bool
	}{
		{
			name: "fuzzy job",
			give: ResponseRestoreState{Kind: RestoreKindFuzzy, NsState: NamespaceStateNone},
			want: true,
		},
		{
			name: "cold job prepared",
			give: ResponseRestoreState{Kind: RestoreKindCold, NsState: NamespaceStateReady},
			want: false,
		},
		{
			name: "cold job preparation failed",
			give: ResponseRestoreState{Kind: RestoreKindCold, NsState: NamespaceStateFailed},
			want: false,
		},
		{
			name: "cold job with effective limits",
			give: ResponseRestoreState{Kind: RestoreKindCold, NsState: NamespaceStateFailed, RestoreWorkerThreads: 8},
			want: true,
		},
		{
			name: "cold job restoring",
			give: ResponseRestoreState{Kind: RestoreKindCold, NsState: NamespaceStateRestoring},
			want: true,
		},
		{
			name: "cold job opening",
			give: ResponseRestoreState{Kind: RestoreKindCold, NsState: NamespaceStateOpening},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.give.IsStarted())
		})
	}
}
