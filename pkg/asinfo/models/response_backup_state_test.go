// Copyright 2024 Aerospike, Inc.
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

func TestNewResponseServerBackupStatus(t *testing.T) {
	t.Parallel()

	startTime, err := time.Parse(infoTimeLayout, "20260805T063119.905Z")
	require.NoError(t, err)

	finishTime, err := time.Parse(infoTimeLayout, "20260805T063131.077Z")
	require.NoError(t, err)

	got := NewResponseBackupState([]InfoMap{
		{
			"change-stream-active": "false",
			"finish-time":          "20260805T063131.077Z",
			"job-id":               "523607479",
			"ns":                   "source-ns1",
			"partitions-flushed":   "4096",
			"partitions-owned":     "4096",
			"partitions-scanned":   "4096",
			"progress-pct":         "100.00",
			"recs-backed-up":       "985",
			"recs-change":          "0",
			"recs-scan":            "985",
			"start-time":           "20260805T063119.905Z",
			"state":                "COMPLETE",
		},
	})

	require.NotNil(t, got)
	assert.Equal(t, &ResponseBackupState{
		JobID:              "523607479",
		Namespace:          "source-ns1",
		State:              "COMPLETE",
		RecsBackedUp:       985,
		RecsScan:           985,
		RecsChange:         0,
		PartitionsFlushed:  4096,
		PartitionsOwned:    4096,
		PartitionsScanned:  4096,
		ProgressPct:        100.00,
		ChangeStreamActive: false,
		StartTime:          startTime,
		FinishTime:         finishTime,
	}, got)
}

func TestNewResponseServerBackupStatus_NoState(t *testing.T) {
	t.Parallel()

	got := NewResponseBackupState([]InfoMap{
		{"job-id": "523607479"},
	})

	assert.Nil(t, got)
}

func TestBackupState_ToHuman(t *testing.T) {
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
		{BackupStateComplete, "backup complete"},
		{BackupStateFailed, "backup failed"},
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

func TestResolveServerBackupState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		states []BackupState
		want   BackupState
	}{
		{
			name:   "failed overrides all",
			states: []BackupState{BackupStateComplete, BackupStateFailed, BackupStateInit},
			want:   BackupStateFailed,
		},
		{
			name:   "lowest lifecycle state wins",
			states: []BackupState{BackupStateComplete, BackupStateBaseScanActive, BackupStateIncrScanActive},
			want:   BackupStateBaseScanActive,
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, ResolveBackupState(tt.states))
		})
	}
}

func TestMergeResponseServerBackupStates(t *testing.T) {
	t.Parallel()

	startEarly := time.Date(2026, 8, 5, 6, 31, 19, 905000000, time.UTC)
	startLate := time.Date(2026, 8, 5, 6, 31, 20, 0, time.UTC)
	finishEarly := time.Date(2026, 8, 5, 6, 31, 30, 0, time.UTC)
	finishLate := time.Date(2026, 8, 5, 6, 31, 31, 77000000, time.UTC)

	got := MergeResponseBackupStates([]*ResponseBackupState{
		{
			JobID:              "523607479",
			Namespace:          "source-ns1",
			State:              BackupStateComplete,
			RecsBackedUp:       500,
			RecsScan:           500,
			RecsChange:         0,
			PartitionsFlushed:  2048,
			PartitionsOwned:    2048,
			PartitionsScanned:  2048,
			ProgressPct:        100,
			ChangeStreamActive: false,
			StartTime:          startLate,
			FinishTime:         finishLate,
		},
		{
			JobID:              "523607479",
			Namespace:          "source-ns1",
			State:              BackupStateBaseScanActive,
			RecsBackedUp:       485,
			RecsScan:           485,
			RecsChange:         0,
			PartitionsFlushed:  1024,
			PartitionsOwned:    2048,
			PartitionsScanned:  1024,
			ProgressPct:        50,
			ChangeStreamActive: true,
			StartTime:          startEarly,
			FinishTime:         finishEarly,
		},
	})

	require.NotNil(t, got)
	assert.Equal(t, &ResponseBackupState{
		JobID:              "523607479",
		Namespace:          "source-ns1",
		State:              BackupStateBaseScanActive,
		RecsBackedUp:       985,
		RecsScan:           985,
		RecsChange:         0,
		PartitionsFlushed:  3072,
		PartitionsOwned:    4096,
		PartitionsScanned:  3072,
		ProgressPct:        75,
		ChangeStreamActive: true,
		StartTime:          startEarly,
		FinishTime:         finishLate,
	}, got)
}
