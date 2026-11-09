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
	testHeldReason    = "truncate-state-live"

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
					fieldHeldReason:             testHeldReason,
					fieldProgressPct:            "47.50",
					fieldSourceConsistency:      string(ConsistencyStrict),
					fieldRecsPerSec:             "300",
					fieldPendingQDepth:          "6",
					fieldPendingQCap:            "64",
					fieldLastLapMs:              "1500",
					fieldTotal:                  "1200",
					fieldApplied:                "1000",
					fieldDeletes:                "40",
					fieldFiltered:               "190",
					fieldFilteredNotOurs:        "150",
					fieldFailed:                 "10",
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
				HeldReason:             testHeldReason,
				ProgressPct:            47.5,
				SourceConsistency:      ConsistencyStrict,
				RecsPerSec:             300,
				PendingQDepth:          6,
				PendingQCap:            64,
				LastLapMs:              new(uint64(1500)),
				Total:                  1200,
				Applied:                1000,
				Deletes:                40,
				Filtered:               190,
				FilteredNotOurs:        150,
				Failed:                 10,
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
					fieldProgressPct:            "100.00",
					fieldSourceConsistency:      string(ConsistencyFuzzy),
					fieldMaxInflight:            "200",
					fieldIgnoreRecordError:      "true",
					fieldLastLapMs:              infoNoValue,
					fieldHeldReason:             infoNoValue,
					fieldTotal:                  "928",
					fieldApplied:                "900",
					fieldDeletes:                "11",
					fieldFiltered:               "24",
					fieldFailed:                 "4",
					fieldOutstanding:            "7",
					fieldCovered:                "1000",
					fieldClusterWide:            "3000",
					fieldBackupID:               testBackupIDIncr1,
					fieldBackupEntry:            "1",
					fieldBackupEntries:          "2",
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
				ProgressPct:            100,
				SourceConsistency:      ConsistencyFuzzy,
				MaxInflight:            200,
				IgnoreRecordError:      true,
				Total:                  928,
				Applied:                900,
				Deletes:                11,
				Filtered:               24,
				Failed:                 4,
				Outstanding:            7,
				Covered:                1000,
				ClusterWide:            3000,
				DeleteAbsent:           6,
			},
		},
		{
			name: "stub row",
			input: []InfoMap{
				{
					fieldJobID: testJobID, fieldNamespace: infoNoName, fieldKind: testKindCold,
					fieldState: string(RestoreStateFailed), fieldNsState: infoNoValue,
					fieldRestoreRPS: "0", fieldRestoreWorkerThreads: "0", fieldRestoreDownloadThreads: "0",
					fieldStartTime: infoNoValue, fieldFinishTime: infoNoValue,
				},
			},
			want: &ResponseRestoreState{
				JobID:     testJobID,
				Namespace: infoNoName,
				Kind:      RestoreKindCold,
				State:     RestoreStateFailed,
				NsState:   NamespaceStateStub,
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
			name:   "stub rows are skipped",
			states: []NamespaceState{NamespaceStateStub, NamespaceStateReady, ""},
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
			name:   "unknown reported by the server overrides ready",
			states: []NamespaceState{NamespaceStateReady, NamespaceStateUnknown},
			want:   NamespaceStateUnknown,
		},
		{
			name:   "stub rows only",
			states: []NamespaceState{NamespaceStateStub},
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
			name:   "failed once every node is terminal",
			states: []RestoreState{RestoreStateAborted, RestoreStateFailed, RestoreStateComplete},
			want:   RestoreStateFailed,
		},
		{
			name:   "aborted once every node is terminal",
			states: []RestoreState{RestoreStateComplete, RestoreStateAborted},
			want:   RestoreStateAborted,
		},
		{
			name:   "aborting while a node aborts",
			states: []RestoreState{RestoreStateAborted, RestoreStateFailed, RestoreStateAborting},
			want:   RestoreStateAborting,
		},
		{
			name:   "aborting while a failed job is still active on a node",
			states: []RestoreState{RestoreStateActive, RestoreStateFailed},
			want:   RestoreStateAborting,
		},
		{
			name:   "aborting while an aborted job is still active on a node",
			states: []RestoreState{RestoreStateActive, RestoreStateAborted},
			want:   RestoreStateAborting,
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
			name:   "failed mixed with a state this package does not know is not hidden",
			states: []RestoreState{RestoreStateFailed, "SOMETHING_NEW"},
			want:   RestoreStateAborting,
		},
		{
			name:   "aborted mixed with unknown is not hidden",
			states: []RestoreState{RestoreStateAborted, RestoreStateUnknown},
			want:   RestoreStateAborting,
		},
		{
			name:   "active mixed with unknown",
			states: []RestoreState{RestoreStateActive, RestoreStateUnknown},
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
					PidsInScope:            2048,
					NDoneNodes:             2,
					ClusterKey:             testClusterKey,
					Done:                   true,
					Clear:                  ClearStateNone,
					ClearZeroedBytes:       1000,
					ClearMs:                900,
					PendingQDepth:          2,
					PendingQCap:            64,
					Total:                  100,
					Applied:                80,
					Deletes:                5,
					Filtered:               15,
					FilteredNotOurs:        12,
					Failed:                 5,
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
					PidsInScope:            2048,
					MissingLayoutNodes:     1,
					NDoneNodes:             1,
					Joiner:                 true,
					Clear:                  ClearStateClearing,
					ClearZeroedBytes:       500,
					ClearMs:                1200,
					HeldReason:             testHeldReason,
					PendingQDepth:          5,
					PendingQCap:            64,
					Total:                  50,
					Applied:                40,
					Deletes:                1,
					Filtered:               10,
					FilteredNotOurs:        8,
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
				HeldReason:             testHeldReason,
				PendingQDepth:          7,
				PendingQCap:            128,
				Total:                  150,
				Applied:                120,
				Deletes:                6,
				Filtered:               25,
				FilteredNotOurs:        20,
				Failed:                 5,
			},
		},
		{
			name: "fuzzy job takes the node with the lowest backup entry and sums counters",
			statuses: []*ResponseRestoreState{
				{
					JobID:             testJobID,
					Namespace:         testNamespace,
					Kind:              RestoreKindFuzzy,
					State:             RestoreStateFailed,
					NsState:           NamespaceStateNone,
					ErrorReason:       testErrorReason,
					BackupID:          testBackupIDIncr2,
					BackupEntry:       2,
					BackupEntries:     3,
					SourceConsistency: ConsistencyStrict,
					MaxInflight:       200,
					RecsPerSec:        10,
					LastLapMs:         new(uint64(900)),
					Total:             18,
					Applied:           10,
					Deletes:           5,
					Filtered:          7,
					Failed:            1,
					Outstanding:       2,
					Covered:           100,
					ClusterWide:       250,
					DeleteAbsent:      1,
				},
				{
					JobID:             testJobID,
					Namespace:         testNamespace,
					Kind:              RestoreKindFuzzy,
					State:             RestoreStateAborted,
					NsState:           NamespaceStateNone,
					ErrorReason:       testAbortReason,
					AbortOrigin:       AbortOriginOperator,
					BackupID:          testBackupIDIncr1,
					BackupEntry:       1,
					BackupEntries:     3,
					SourceConsistency: ConsistencyFuzzy,
					MaxInflight:       200,
					IgnoreRecordError: true,
					RecsPerSec:        20,
					Total:             39,
					Applied:           30,
					Deletes:           2,
					Filtered:          7,
					Failed:            2,
					Outstanding:       1,
					Covered:           150,
					ClusterWide:       300,
					DeleteAbsent:      2,
				},
			},
			want: &ResponseRestoreState{
				JobID:             testJobID,
				Namespace:         testNamespace,
				Kind:              RestoreKindFuzzy,
				State:             RestoreStateFailed,
				NsState:           NamespaceStateNone,
				ErrorReason:       testErrorReason,
				AbortOrigin:       AbortOriginOperator,
				BackupID:          testBackupIDIncr1,
				BackupEntry:       1,
				BackupEntries:     3,
				SourceConsistency: ConsistencyFuzzy,
				MaxInflight:       200,
				IgnoreRecordError: true,
				RecsPerSec:        30,
				LastLapMs:         new(uint64(900)),
				Total:             57,
				Applied:           40,
				Deletes:           7,
				Filtered:          14,
				Failed:            3,
				Outstanding:       3,
				Covered:           250,
				ClusterWide:       300,
				DeleteAbsent:      3,
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
					NsState: NamespaceStateStub,
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
			name: "job id and namespace are taken from the first row that is not a stub",
			statuses: []*ResponseRestoreState{
				{
					JobID:     infoNoName,
					Namespace: infoNoName,
					Kind:      RestoreKindCold,
					State:     RestoreStateFailed,
					NsState:   NamespaceStateStub,
				},
				{
					JobID:     testJobID,
					Namespace: testNamespace,
					Kind:      RestoreKindCold,
					State:     RestoreStateFailed,
					NsState:   NamespaceStateFailed,
				},
			},
			want: &ResponseRestoreState{
				JobID:     testJobID,
				Namespace: testNamespace,
				Kind:      RestoreKindCold,
				State:     RestoreStateFailed,
				NsState:   NamespaceStateFailed,
			},
		},
		{
			name: "finish time is zero while a node has not finished",
			statuses: []*ResponseRestoreState{
				{
					JobID:      testJobID,
					Namespace:  testNamespace,
					Kind:       RestoreKindCold,
					State:      RestoreStateComplete,
					FinishTime: testFinishTime,
				},
				{
					JobID:     testJobID,
					Namespace: testNamespace,
					Kind:      RestoreKindCold,
					State:     RestoreStateActive,
				},
			},
			want: &ResponseRestoreState{
				JobID:     testJobID,
				Namespace: testNamespace,
				Kind:      RestoreKindCold,
				State:     RestoreStateActive,
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

func TestMergeResponseRestoreStates_ProgressPct(t *testing.T) {
	t.Parallel()

	cold := func(pct float64, pids uint32) *ResponseRestoreState {
		return &ResponseRestoreState{Kind: RestoreKindCold, ProgressPct: pct, PidsInScope: pids}
	}

	fuzzy := func(pct float64) *ResponseRestoreState {
		return &ResponseRestoreState{Kind: RestoreKindFuzzy, ProgressPct: pct}
	}

	tests := []struct {
		name     string
		statuses []*ResponseRestoreState
		want     float64
	}{
		{
			name:     "cold nodes with equal shares",
			statuses: []*ResponseRestoreState{cold(100, 2048), cold(50, 2048)},
			want:     75,
		},
		{
			name:     "cold nodes are weighted by their partitions in scope",
			statuses: []*ResponseRestoreState{cold(100, 3072), cold(20, 1024)},
			want:     80,
		},
		{
			name:     "cold node without partitions in scope is skipped",
			statuses: []*ResponseRestoreState{cold(60, 4096), cold(0, 0)},
			want:     60,
		},
		{
			name:     "cold nodes without partitions in scope report no progress",
			statuses: []*ResponseRestoreState{cold(0, 0), cold(0, 0)},
			want:     0,
		},
		{
			name:     "fuzzy nodes weigh the same",
			statuses: []*ResponseRestoreState{fuzzy(95), fuzzy(45), nil, fuzzy(10)},
			want:     50,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := MergeResponseRestoreStates(tt.statuses)

			require.NotNil(t, got)
			assert.InDelta(t, tt.want, got.ProgressPct, 1e-9)
		})
	}
}
