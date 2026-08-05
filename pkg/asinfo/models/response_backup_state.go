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
	"slices"
	"time"
)

const infoTimeLayout = "20060102T150405.000Z"

type BackupState string

const (
	BackupStateInit                 BackupState = "INIT"
	BackupStateBaseScanActive       BackupState = "BASE_SCAN_ACTIVE"
	BackupStateBaseScanDone         BackupState = "BASE_SCAN_DONE"
	BackupStateIncrScanActive       BackupState = "INCR_SCAN_ACTIVE"
	BackupStateStoppingChangeStream BackupState = "STOPPING_CHANGE_STREAM"
	BackupStateFinalDraining        BackupState = "FINAL_DRAINING"
	BackupStateComplete             BackupState = "COMPLETE"
	BackupStateFailed               BackupState = "FAILED"
	BackupStateUnknown              BackupState = "UNKNOWN"
)

// backupStateOrder defines backup lifecycle order. Lower index = earlier stage.
var backupStateOrder = []BackupState{
	BackupStateInit,
	BackupStateBaseScanActive,
	BackupStateBaseScanDone,
	BackupStateIncrScanActive,
	BackupStateStoppingChangeStream,
	BackupStateFinalDraining,
	BackupStateComplete,
}

// ResponseBackupState represents the status of a server-side backup job.
type ResponseBackupState struct {
	JobID              string
	Namespace          string
	State              BackupState
	RecsBackedUp       int
	RecsScan           int
	RecsChange         int
	PartitionsFlushed  int
	PartitionsOwned    int
	PartitionsScanned  int
	ProgressPct        float64
	ChangeStreamActive bool
	StartTime          time.Time
	FinishTime         time.Time
}

// NewResponseBackupState builds a ResponseBackupState from the first
// InfoMap entry that contains a state field.
func NewResponseBackupState(im []InfoMap) *ResponseBackupState {
	for _, r := range im {
		state, ok := r["state"]
		if !ok {
			continue
		}

		status := &ResponseBackupState{
			JobID:     r["job-id"],
			Namespace: r["ns"],
			State:     BackupState(state),
		}

		if v, ok, _ := r.ParseInt64("recs-backed-up"); ok {
			status.RecsBackedUp = int(v)
		}

		if v, ok, _ := r.ParseInt64("recs-scan"); ok {
			status.RecsScan = int(v)
		}

		if v, ok, _ := r.ParseInt64("recs-change"); ok {
			status.RecsChange = int(v)
		}

		if v, ok, _ := r.ParseInt64("partitions-flushed"); ok {
			status.PartitionsFlushed = int(v)
		}

		if v, ok, _ := r.ParseInt64("partitions-owned"); ok {
			status.PartitionsOwned = int(v)
		}

		if v, ok, _ := r.ParseInt64("partitions-scanned"); ok {
			status.PartitionsScanned = int(v)
		}

		if v, ok, _ := r.ParseFloat64("progress-pct"); ok {
			status.ProgressPct = v
		}

		if v, ok, _ := r.ParseBool("change-stream-active"); ok {
			status.ChangeStreamActive = v
		}

		if val, ok := r["start-time"]; ok {
			if t, err := time.Parse(infoTimeLayout, val); err == nil {
				status.StartTime = t
			}
		}

		if val, ok := r["finish-time"]; ok {
			if t, err := time.Parse(infoTimeLayout, val); err == nil {
				status.FinishTime = t
			}
		}

		return status
	}

	return nil
}

// ResolveBackupState returns the combined backup state across nodes.
// If any node reports FAILED, FAILED is returned. Otherwise the earliest
// lifecycle state among all nodes is returned.
func ResolveBackupState(states []BackupState) BackupState {
	if slices.Contains(states, BackupStateFailed) {
		return BackupStateFailed
	}

	var (
		resolved     BackupState
		resolvedRank = len(backupStateOrder)
	)

	for _, state := range states {
		rank, ok := backupStateRank(state)
		if !ok {
			continue
		}

		if rank < resolvedRank {
			resolvedRank = rank
			resolved = state
		}
	}

	if resolvedRank == len(backupStateOrder) {
		return BackupStateUnknown
	}

	return resolved
}

func backupStateRank(state BackupState) (int, bool) {
	for i, ordered := range backupStateOrder {
		if state == ordered {
			return i, true
		}
	}

	return 0, false
}

// MergeResponseBackupStates combines per-node backup status responses
// into a single cluster-wide view. StartTime is the earliest timestamp across
// nodes; FinishTime is the latest.
func MergeResponseBackupStates(statuses []*ResponseBackupState) *ResponseBackupState {
	valid := make([]*ResponseBackupState, 0, len(statuses))
	for _, status := range statuses {
		if status != nil {
			valid = append(valid, status)
		}
	}

	if len(valid) == 0 {
		return nil
	}

	merged := &ResponseBackupState{
		JobID:     valid[0].JobID,
		Namespace: valid[0].Namespace,
	}

	states := make([]BackupState, 0, len(valid))

	var progressWeightedSum float64

	for _, status := range valid {
		states = append(states, status.State)

		merged.RecsBackedUp += status.RecsBackedUp
		merged.RecsScan += status.RecsScan
		merged.RecsChange += status.RecsChange
		merged.PartitionsFlushed += status.PartitionsFlushed
		merged.PartitionsOwned += status.PartitionsOwned
		merged.PartitionsScanned += status.PartitionsScanned
		merged.ChangeStreamActive = merged.ChangeStreamActive || status.ChangeStreamActive

		if status.PartitionsOwned > 0 {
			progressWeightedSum += status.ProgressPct * float64(status.PartitionsOwned)
		}

		if !status.StartTime.IsZero() &&
			(merged.StartTime.IsZero() || status.StartTime.Before(merged.StartTime)) {
			merged.StartTime = status.StartTime
		}

		if !status.FinishTime.IsZero() &&
			(merged.FinishTime.IsZero() || status.FinishTime.After(merged.FinishTime)) {
			merged.FinishTime = status.FinishTime
		}
	}

	merged.State = ResolveBackupState(states)

	// Each node reports its own progress-pct for the partitions it owns.
	// Those percentages aren't equally sized slices of the cluster job
	// unless every node owns the same number of partitions.
	if merged.PartitionsOwned > 0 {
		merged.ProgressPct = progressWeightedSum / float64(merged.PartitionsOwned)
	}

	return merged
}
