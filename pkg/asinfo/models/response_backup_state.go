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
	"slices"
	"time"
)

const (
	// infoTimeLayout is the ISO 8601 basic UTC layout the server uses for the
	// backup job timestamps, e.g. 20260922T134400.123Z.
	infoTimeLayout = "20060102T150405.000Z"

	// infoNoValue is emitted by the server in place of a timestamp that is not
	// set yet, e.g. the finish-time of a running job.
	infoNoValue = "-"
)

// Keys of the "backup-status" info response. The optional fields
// drain-blocked-migrations and error-reason appear in the middle of the
// record, so the response must be read by key and never by position.
const (
	fieldJobID                  = "job-id"
	fieldNamespace              = "ns"
	fieldState                  = "state"
	fieldRecsBackedUp           = "recs-backed-up"
	fieldRecsBase               = "recs-base"
	fieldRecsIncr               = "recs-incr"
	fieldRecsChange             = "recs-change"
	fieldRecsFiltered           = "recs-filtered"
	fieldRecsDegenerate         = "recs-degenerate"
	fieldRecsSkippedXDRTomb     = "recs-skipped-xdr-tomb"
	fieldRecsReadFailed         = "recs-read-failed"
	fieldPartitionsFlushed      = "partitions-flushed"
	fieldPartitionsOwned        = "partitions-owned"
	fieldPartitionsScanned      = "partitions-scanned"
	fieldProgressPct            = "progress-pct"
	fieldChangeStreamActive     = "change-stream-active"
	fieldStartTime              = "start-time"
	fieldFinishTime             = "finish-time"
	fieldDrainBlockedMigrations = "drain-blocked-migrations"
	fieldErrorReason            = "error-reason"
	fieldPartitionsCountPending = "partitions-count-pending"
	fieldCountReadFailures      = "count-read-failures"
)

type BackupState string

const (
	BackupStateInit                 BackupState = "INIT"
	BackupStateBaseScanActive       BackupState = "BASE_SCAN_ACTIVE"
	BackupStateBaseScanDone         BackupState = "BASE_SCAN_DONE"
	BackupStateIncrScanActive       BackupState = "INCR_SCAN_ACTIVE"
	BackupStateStoppingChangeStream BackupState = "STOPPING_CHANGE_STREAM"
	BackupStateFinalDraining        BackupState = "FINAL_DRAINING"
	BackupStateCommitting           BackupState = "COMMITTING"
	BackupStateComplete             BackupState = "COMPLETE"
	BackupStateFailed               BackupState = "FAILED"
	// BackupStateAborting is synthetic: the server reports it once an abort has
	// been requested but the job has not reached a terminal state yet.
	BackupStateAborting BackupState = "ABORTING"
	BackupStateAborted  BackupState = "ABORTED"
	BackupStateUnknown  BackupState = "UNKNOWN"
)

// Describe returns a human-readable description of the backup state.
func (s BackupState) Describe() string {
	switch s {
	case BackupStateInit:
		return "initializing backup"
	case BackupStateBaseScanActive:
		return "scanning disk and backing up all records"
	case BackupStateBaseScanDone:
		return "base scan complete"
	case BackupStateIncrScanActive:
		return "capturing live writes and scanning for records updated since base scan"
	case BackupStateStoppingChangeStream:
		return "stopping change stream capture"
	case BackupStateFinalDraining:
		return "flushing buffered backup segments to object storage"
	case BackupStateCommitting:
		return "committing backup metadata"
	case BackupStateComplete:
		return "backup complete"
	case BackupStateFailed:
		return "backup failed"
	case BackupStateAborting:
		return "aborting backup"
	case BackupStateAborted:
		return "backup aborted"
	case BackupStateUnknown:
		return "unknown backup state"
	default:
		return "unknown backup state: " + string(s)
	}
}

// backupStateOrder defines backup lifecycle order. Lower index = earlier stage.
// The terminal states FAILED, ABORTING and ABORTED are not part of it: they are
// resolved by priority in [ResolveBackupState].
var backupStateOrder = []BackupState{
	BackupStateInit,
	BackupStateBaseScanActive,
	BackupStateBaseScanDone,
	BackupStateIncrScanActive,
	BackupStateStoppingChangeStream,
	BackupStateFinalDraining,
	BackupStateCommitting,
	BackupStateComplete,
}

// ResponseBackupState represents the status of a server-side backup job,
// as reported by the "backup-status" info command.
type ResponseBackupState struct {
	// StartTime is zero while the job has not started yet.
	StartTime time.Time
	// FinishTime is zero while the job has not finished yet.
	FinishTime time.Time

	JobID     string
	Namespace string
	// ErrorReason is only reported by the server when the job carries one.
	ErrorReason string
	State       BackupState

	// RecsBackedUp is the sum of RecsBase, RecsIncr and RecsChange. It counts
	// durable records over all three lines rather than distinct records, so it
	// is not monotonic.
	RecsBackedUp uint64
	// RecsBase counts durable records of the base scan. It is understated while
	// PartitionsCountPending is non-zero.
	RecsBase uint64
	// RecsIncr counts durable records of the incremental scan.
	RecsIncr uint64
	// RecsChange counts durable records of the change stream.
	RecsChange uint64
	// RecsFiltered counts records dropped by the set or expression filter.
	RecsFiltered uint64
	// RecsDegenerate counts degenerate records.
	RecsDegenerate uint64
	// RecsSkippedXDRTomb counts skipped XDR tombstones.
	RecsSkippedXDRTomb uint64
	// RecsReadFailed counts records that failed to be read.
	RecsReadFailed uint64
	// DrainBlockedMigrations is only reported while the job drains and
	// migrations hold it back.
	DrainBlockedMigrations uint64
	// CountReadFailures counts manifest read failures of the base count.
	CountReadFailures uint64

	// PartitionsFlushed counts partitions flushed to the object store.
	PartitionsFlushed uint32
	// PartitionsOwned counts partitions owned by the node.
	PartitionsOwned uint32
	// PartitionsScanned counts partitions already scanned.
	PartitionsScanned uint32
	// PartitionsCountPending counts partitions with no base count received yet.
	PartitionsCountPending uint32

	ProgressPct        float64
	ChangeStreamActive bool
}

// NewResponseBackupState builds a ResponseBackupState from the first
// InfoMap entry that contains a state field.
func NewResponseBackupState(im []InfoMap) *ResponseBackupState {
	for _, r := range im {
		state, ok := r[fieldState]
		if !ok {
			continue
		}

		status := &ResponseBackupState{
			JobID:       r[fieldJobID],
			Namespace:   r[fieldNamespace],
			ErrorReason: r[fieldErrorReason],
			State:       BackupState(state),
			StartTime:   parseInfoTime(r[fieldStartTime]),
			FinishTime:  parseInfoTime(r[fieldFinishTime]),
		}

		status.parseCounters(r)

		if v, ok, _ := r.ParseFloat64(fieldProgressPct); ok {
			status.ProgressPct = v
		}

		if v, ok, _ := r.ParseBool(fieldChangeStreamActive); ok {
			status.ChangeStreamActive = v
		}

		return status
	}

	return nil
}

// parseCounters fills the numeric counters from the info map. A counter the
// server did not report, or reported in an unparsable form, is left at zero:
// the optional fields of the response make a missing key an expected case, and
// a single bad counter must not discard the rest of the status.
func (s *ResponseBackupState) parseCounters(r InfoMap) {
	uint64Fields := []struct {
		key string
		dst *uint64
	}{
		{fieldRecsBackedUp, &s.RecsBackedUp},
		{fieldRecsBase, &s.RecsBase},
		{fieldRecsIncr, &s.RecsIncr},
		{fieldRecsChange, &s.RecsChange},
		{fieldRecsFiltered, &s.RecsFiltered},
		{fieldRecsDegenerate, &s.RecsDegenerate},
		{fieldRecsSkippedXDRTomb, &s.RecsSkippedXDRTomb},
		{fieldRecsReadFailed, &s.RecsReadFailed},
		{fieldDrainBlockedMigrations, &s.DrainBlockedMigrations},
		{fieldCountReadFailures, &s.CountReadFailures},
	}

	for _, f := range uint64Fields {
		if v, ok, _ := r.ParseUint64(f.key); ok {
			*f.dst = v
		}
	}

	uint32Fields := []struct {
		key string
		dst *uint32
	}{
		{fieldPartitionsFlushed, &s.PartitionsFlushed},
		{fieldPartitionsOwned, &s.PartitionsOwned},
		{fieldPartitionsScanned, &s.PartitionsScanned},
		{fieldPartitionsCountPending, &s.PartitionsCountPending},
	}

	for _, f := range uint32Fields {
		if v, ok, _ := r.ParseUint32(f.key); ok {
			*f.dst = v
		}
	}
}

// parseInfoTime parses a timestamp of the info response. The server emits
// infoNoValue for a timestamp that is not set yet, which yields the zero time,
// as does any value that does not match the expected layout.
func parseInfoTime(val string) time.Time {
	if val == "" || val == infoNoValue {
		return time.Time{}
	}

	t, err := time.Parse(infoTimeLayout, val)
	if err != nil {
		return time.Time{}
	}

	return t
}

// ResolveBackupState returns the combined backup state across nodes.
// Terminal states win over lifecycle states: FAILED on any node makes the whole
// job failed, and a requested abort makes it ABORTING until the last node has
// reached ABORTED. Otherwise the earliest lifecycle state among all nodes is
// returned, because the job as a whole is only as far as its slowest node.
func ResolveBackupState(states []BackupState) BackupState {
	switch {
	case slices.Contains(states, BackupStateFailed):
		return BackupStateFailed
	case slices.Contains(states, BackupStateAborting):
		return BackupStateAborting
	case slices.Contains(states, BackupStateAborted):
		return BackupStateAborted
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
// into a single cluster-wide view. Counters are summed, StartTime is the
// earliest timestamp across nodes, FinishTime is the latest, and ErrorReason is
// the first one reported by any node.
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
		merged.RecsBase += status.RecsBase
		merged.RecsIncr += status.RecsIncr
		merged.RecsChange += status.RecsChange
		merged.RecsFiltered += status.RecsFiltered
		merged.RecsDegenerate += status.RecsDegenerate
		merged.RecsSkippedXDRTomb += status.RecsSkippedXDRTomb
		merged.RecsReadFailed += status.RecsReadFailed
		merged.DrainBlockedMigrations += status.DrainBlockedMigrations
		merged.CountReadFailures += status.CountReadFailures
		merged.PartitionsFlushed += status.PartitionsFlushed
		merged.PartitionsOwned += status.PartitionsOwned
		merged.PartitionsScanned += status.PartitionsScanned
		merged.PartitionsCountPending += status.PartitionsCountPending
		merged.ChangeStreamActive = merged.ChangeStreamActive || status.ChangeStreamActive

		if merged.ErrorReason == "" {
			merged.ErrorReason = status.ErrorReason
		}

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
