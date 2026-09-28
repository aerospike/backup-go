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

const (
	testJobID     = "260922T103653-5xsr"
	testNamespace = "source-ns1"

	testStartTimeStr  = "20260922T103653.415Z"
	testFinishTimeStr = "20260922T104120.077Z"

	testErrorReason = "storage unreachable"
)

var (
	testStartTime  = time.Date(2026, 9, 22, 10, 36, 53, 415000000, time.UTC)
	testFinishTime = time.Date(2026, 9, 22, 10, 41, 20, 77000000, time.UTC)
)

func TestNewResponseBackupState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input []InfoMap
		want  *ResponseBackupState
	}{
		{
			name: "running job, optional fields absent",
			input: []InfoMap{
				{
					"job-id":                   testJobID,
					"ns":                       testNamespace,
					"state":                    "INCR_SCAN_ACTIVE",
					"recs-base":                "515423",
					"recs-incr":                "0",
					"recs-change":              "0",
					"recs-filtered":            "0",
					"recs-skipped-xdr-tomb":    "0",
					"recs-read-failed":         "0",
					"partitions-flushed":       "1365",
					"partitions-owned":         "1365",
					"partitions-scanned":       "1365",
					"progress-pct":             "95.00",
					"change-stream-active":     "true",
					"start-time":               testStartTimeStr,
					"finish-time":              "-",
					"partitions-count-pending": "0",
					"count-read-failures":      "0",
				},
			},
			want: &ResponseBackupState{
				JobID:              testJobID,
				Namespace:          testNamespace,
				State:              BackupStateIncrScanActive,
				RecsBase:           515423,
				PartitionsFlushed:  1365,
				PartitionsOwned:    1365,
				PartitionsScanned:  1365,
				ProgressPct:        95.00,
				ChangeStreamActive: true,
				StartTime:          testStartTime,
			},
		},
		{
			name: "completed job with every field reported",
			input: []InfoMap{
				{
					"job-id":                   testJobID,
					"ns":                       testNamespace,
					"state":                    "COMPLETE",
					"recs-backed-up":           "1200",
					"recs-base":                "1000",
					"recs-incr":                "150",
					"recs-change":              "50",
					"recs-filtered":            "7",
					"recs-degenerate":          "3",
					"recs-skipped-xdr-tomb":    "2",
					"recs-read-failed":         "1",
					"partitions-flushed":       "4096",
					"partitions-owned":         "4096",
					"partitions-scanned":       "4096",
					"progress-pct":             "100.00",
					"change-stream-active":     "false",
					"start-time":               testStartTimeStr,
					"finish-time":              testFinishTimeStr,
					"partitions-count-pending": "5",
					"count-read-failures":      "4",
				},
			},
			want: &ResponseBackupState{
				JobID:                  testJobID,
				Namespace:              testNamespace,
				State:                  BackupStateComplete,
				RecsBackedUp:           1200,
				RecsBase:               1000,
				RecsIncr:               150,
				RecsChange:             50,
				RecsFiltered:           7,
				RecsDegenerate:         3,
				RecsSkippedXDRTomb:     2,
				RecsReadFailed:         1,
				PartitionsFlushed:      4096,
				PartitionsOwned:        4096,
				PartitionsScanned:      4096,
				PartitionsCountPending: 5,
				CountReadFailures:      4,
				ProgressPct:            100.00,
				StartTime:              testStartTime,
				FinishTime:             testFinishTime,
			},
		},
		{
			name: "conditional drain-blocked-migrations is read by key",
			input: []InfoMap{
				{
					"job-id":                   testJobID,
					"ns":                       testNamespace,
					"state":                    "FINAL_DRAINING",
					"drain-blocked-migrations": "12",
					"start-time":               testStartTimeStr,
					"finish-time":              "-",
				},
			},
			want: &ResponseBackupState{
				JobID:                  testJobID,
				Namespace:              testNamespace,
				State:                  BackupStateFinalDraining,
				DrainBlockedMigrations: 12,
				StartTime:              testStartTime,
			},
		},
		{
			name: "conditional error-reason is read by key",
			input: []InfoMap{
				{
					"job-id":       testJobID,
					"ns":           testNamespace,
					"state":        "FAILED",
					"error-reason": testErrorReason,
					"start-time":   testStartTimeStr,
					"finish-time":  testFinishTimeStr,
				},
			},
			want: &ResponseBackupState{
				JobID:       testJobID,
				Namespace:   testNamespace,
				State:       BackupStateFailed,
				ErrorReason: testErrorReason,
				StartTime:   testStartTime,
				FinishTime:  testFinishTime,
			},
		},
		{
			name: "job that has not started yet",
			input: []InfoMap{
				{
					"job-id":      testJobID,
					"ns":          testNamespace,
					"state":       "INIT",
					"start-time":  "-",
					"finish-time": "-",
				},
			},
			want: &ResponseBackupState{
				JobID:     testJobID,
				Namespace: testNamespace,
				State:     BackupStateInit,
			},
		},
		{
			name: "unparsable counter leaves the field at zero",
			input: []InfoMap{
				{
					"job-id":             testJobID,
					"state":              "BASE_SCAN_ACTIVE",
					"recs-base":          "not-a-number",
					"partitions-owned":   "also-not-a-number",
					"partitions-flushed": "10",
				},
			},
			want: &ResponseBackupState{
				JobID:             testJobID,
				State:             BackupStateBaseScanActive,
				PartitionsFlushed: 10,
			},
		},
		{
			name: "first entry with a state wins",
			input: []InfoMap{
				{"job-id": testJobID},
				{"job-id": testJobID, "ns": testNamespace, "state": "COMMITTING"},
			},
			want: &ResponseBackupState{
				JobID:     testJobID,
				Namespace: testNamespace,
				State:     BackupStateCommitting,
			},
		},
		{
			name:  "no state at all",
			input: []InfoMap{{"job-id": testJobID}},
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

			assert.Equal(t, tt.want, NewResponseBackupState(tt.input))
		})
	}
}

func TestBackupState_Describe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		state BackupState
		want  string
	}{
		{BackupStateInit, "initializing backup"},
		{BackupStateBaseScanActive, "scanning disk and backing up all records"},
		{BackupStateBaseScanDone, "base scan complete"},
		{
			BackupStateIncrScanActive,
			"capturing live writes and scanning for records updated since base scan",
		},
		{BackupStateStoppingChangeStream, "stopping change stream capture"},
		{BackupStateFinalDraining, "flushing buffered backup segments to object storage"},
		{BackupStateCommitting, "committing backup metadata"},
		{BackupStateComplete, "backup complete"},
		{BackupStateFailed, "backup failed"},
		{BackupStateAborting, "aborting backup"},
		{BackupStateAborted, "backup aborted"},
		{BackupStateUnknown, "unknown backup state"},
		{BackupState("CUSTOM"), "unknown backup state: CUSTOM"},
	}

	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.state.Describe())
		})
	}
}

func TestResolveBackupState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		states []BackupState
		want   BackupState
	}{
		{
			name:   "failed overrides all",
			states: []BackupState{BackupStateComplete, BackupStateFailed, BackupStateAborted},
			want:   BackupStateFailed,
		},
		{
			name:   "aborting overrides aborted and lifecycle states",
			states: []BackupState{BackupStateAborted, BackupStateAborting, BackupStateInit},
			want:   BackupStateAborting,
		},
		{
			name:   "aborted overrides lifecycle states",
			states: []BackupState{BackupStateComplete, BackupStateAborted},
			want:   BackupStateAborted,
		},
		{
			name:   "lowest lifecycle state wins",
			states: []BackupState{BackupStateComplete, BackupStateBaseScanActive, BackupStateIncrScanActive},
			want:   BackupStateBaseScanActive,
		},
		{
			name:   "committing precedes complete",
			states: []BackupState{BackupStateComplete, BackupStateCommitting},
			want:   BackupStateCommitting,
		},
		{
			name:   "all complete",
			states: []BackupState{BackupStateComplete, BackupStateComplete},
			want:   BackupStateComplete,
		},
		{
			name:   "unknown when no known states",
			states: []BackupState{BackupStateUnknown},
			want:   BackupStateUnknown,
		},
		{
			name:   "unknown when no states at all",
			states: nil,
			want:   BackupStateUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, ResolveBackupState(tt.states))
		})
	}
}

func TestMergeResponseBackupStates(t *testing.T) {
	t.Parallel()

	startEarly := testStartTime
	startLate := testStartTime.Add(time.Second)
	finishEarly := testFinishTime.Add(-time.Second)
	finishLate := testFinishTime

	tests := []struct {
		name     string
		statuses []*ResponseBackupState
		want     *ResponseBackupState
	}{
		{
			name: "counters summed, earliest start and latest finish kept",
			statuses: []*ResponseBackupState{
				{
					JobID:                  testJobID,
					Namespace:              testNamespace,
					State:                  BackupStateComplete,
					RecsBackedUp:           500,
					RecsBase:               400,
					RecsIncr:               60,
					RecsChange:             40,
					RecsFiltered:           5,
					RecsDegenerate:         2,
					RecsSkippedXDRTomb:     1,
					RecsReadFailed:         3,
					DrainBlockedMigrations: 4,
					CountReadFailures:      6,
					PartitionsFlushed:      2048,
					PartitionsOwned:        2048,
					PartitionsScanned:      2048,
					PartitionsCountPending: 7,
					ProgressPct:            100,
					StartTime:              startLate,
					FinishTime:             finishLate,
				},
				{
					JobID:                  testJobID,
					Namespace:              testNamespace,
					State:                  BackupStateBaseScanActive,
					RecsBackedUp:           485,
					RecsBase:               400,
					RecsIncr:               45,
					RecsChange:             40,
					RecsFiltered:           5,
					RecsDegenerate:         2,
					RecsSkippedXDRTomb:     1,
					RecsReadFailed:         3,
					DrainBlockedMigrations: 4,
					CountReadFailures:      6,
					PartitionsFlushed:      1024,
					PartitionsOwned:        2048,
					PartitionsScanned:      1024,
					PartitionsCountPending: 7,
					ProgressPct:            50,
					ChangeStreamActive:     true,
					StartTime:              startEarly,
					FinishTime:             finishEarly,
				},
			},
			want: &ResponseBackupState{
				JobID:                  testJobID,
				Namespace:              testNamespace,
				State:                  BackupStateBaseScanActive,
				RecsBackedUp:           985,
				RecsBase:               800,
				RecsIncr:               105,
				RecsChange:             80,
				RecsFiltered:           10,
				RecsDegenerate:         4,
				RecsSkippedXDRTomb:     2,
				RecsReadFailed:         6,
				DrainBlockedMigrations: 8,
				CountReadFailures:      12,
				PartitionsFlushed:      3072,
				PartitionsOwned:        4096,
				PartitionsScanned:      3072,
				PartitionsCountPending: 14,
				ProgressPct:            75,
				ChangeStreamActive:     true,
				StartTime:              startEarly,
				FinishTime:             finishLate,
			},
		},
		{
			name: "first reported error reason is kept",
			statuses: []*ResponseBackupState{
				{JobID: testJobID, Namespace: testNamespace, State: BackupStateBaseScanActive},
				{JobID: testJobID, Namespace: testNamespace, State: BackupStateFailed, ErrorReason: testErrorReason},
			},
			want: &ResponseBackupState{
				JobID:       testJobID,
				Namespace:   testNamespace,
				State:       BackupStateFailed,
				ErrorReason: testErrorReason,
			},
		},
		{
			name: "nil entries are skipped",
			statuses: []*ResponseBackupState{
				nil,
				{JobID: testJobID, Namespace: testNamespace, State: BackupStateInit},
			},
			want: &ResponseBackupState{
				JobID:     testJobID,
				Namespace: testNamespace,
				State:     BackupStateInit,
			},
		},
		{
			name:     "no statuses",
			statuses: nil,
			want:     nil,
		},
		{
			name:     "only nil statuses",
			statuses: []*ResponseBackupState{nil, nil},
			want:     nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := MergeResponseBackupStates(tt.statuses)
			if tt.want == nil {
				require.Nil(t, got)
				return
			}

			require.NotNil(t, got)
			assert.Equal(t, tt.want, got)
		})
	}
}
