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

// Keys of the "backup-status" info response, besides the shared ones of
// job_status.go.
const (
	fieldBackupRPS                    = "backup-rps"
	fieldBackupWorkerThreads          = "backup-worker-threads"
	fieldBackupUploadThreads          = "backup-upload-threads"
	fieldBackupS3ThroughputTargetGbps = "backup-s3-throughput-target-gbps"
	fieldRecsBackedUp                 = "recs-backed-up"
	fieldRecsBase                     = "recs-base"
	fieldRecsQueryIncr                = "recs-query-incr"
	fieldRecsChangeIncr               = "recs-change-incr"
	fieldBytesBackedUp                = "bytes-backed-up"
	fieldBytesBase                    = "bytes-base"
	fieldBytesQueryIncr               = "bytes-query-incr"
	fieldBytesChangeIncr              = "bytes-change-incr"
	fieldRecsFiltered                 = "recs-filtered"
	fieldRecsDegenerate               = "recs-degenerate"
	fieldRecsSkippedXDRTomb           = "recs-skipped-xdr-tomb"
	fieldRecsReadFailed               = "recs-read-failed"
	fieldPartitionsFlushed            = "partitions-flushed"
	fieldPartitionsOwned              = "partitions-owned"
	fieldPartitionsScanned            = "partitions-scanned"
	fieldProgressPct                  = "progress-pct"
	fieldChangeStreamActive           = "change-stream-active"
	fieldDrainBlockedMigrations       = "drain-blocked-migrations"
	fieldPartitionsCountPending       = "partitions-count-pending"
	fieldCountReadFailures            = "count-read-failures"
	fieldBackpressureActive           = "backpressure-active"
	fieldBackpressureActivations      = "backpressure-activations"
	fieldBytesInPerSec                = "bytes-in-per-sec"
	fieldBytesOutPerSec               = "bytes-out-per-sec"
	fieldUploadQDepth                 = "upload-q-depth"
	fieldUploadQCap                   = "upload-q-cap"
	fieldBuffersInUse                 = "buffers-in-use"
	fieldTotalBuffersAllocated        = "total-buffers-allocated"
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
	// BackupStateAborting is reported once an abort has been accepted and the
	// discard is still in progress.
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
// as reported by the "backup-status" info command. The figures of a single
// node cover the partitions that node is responsible for.
type ResponseBackupState struct {
	// StartTime is zero while the job has not started yet.
	StartTime time.Time
	// FinishTime is zero while the job has not finished yet.
	FinishTime time.Time

	JobID     string
	Namespace string
	// ErrorReason is only reported by the server when the job failed or was aborted.
	ErrorReason string
	State       BackupState
	// AbortOrigin is only reported by the server when the job was aborted.
	AbortOrigin AbortOrigin

	// BackupRPS is the effective records per second limit, 0 for no limit.
	BackupRPS uint32
	// BackupWorkerThreads is the effective number of scan threads per node.
	BackupWorkerThreads uint32
	// BackupUploadThreads is the effective number of upload threads per node.
	BackupUploadThreads uint32
	// BackupS3ThroughputTargetGbps is the effective S3 throughput target per node.
	BackupS3ThroughputTargetGbps uint32

	// RecsBackedUp is the sum of RecsBase, RecsQueryIncr and RecsChangeIncr. It
	// counts durable records over all three lines rather than distinct records,
	// so it is not monotonic.
	RecsBackedUp uint64
	// RecsBase counts records of the base (full) scan. It is understated while
	// PartitionsCountPending is non-zero.
	RecsBase uint64
	// RecsQueryIncr counts records of the incremental scan.
	RecsQueryIncr uint64
	// RecsChangeIncr counts records captured from writes during the backup.
	RecsChangeIncr uint64
	// BytesBackedUp is the sum of BytesBase, BytesQueryIncr and BytesChangeIncr.
	BytesBackedUp uint64
	// BytesBase counts bytes of the base scan.
	BytesBase uint64
	// BytesQueryIncr counts bytes of the incremental scan.
	BytesQueryIncr uint64
	// BytesChangeIncr counts bytes of the writes captured during the backup.
	BytesChangeIncr uint64
	// RecsFiltered counts records dropped by the filter expression or by an
	// empty bin list projection. With either set, it also counts migrating
	// records that could not be filtered and were dropped.
	RecsFiltered uint64
	// RecsDegenerate counts captured writes with no set name or no bins.
	RecsDegenerate uint64
	// RecsSkippedXDRTomb counts XDR tombstones skipped by the scans.
	RecsSkippedXDRTomb uint64
	// RecsReadFailed counts records that could not be read. A non-zero value
	// in a complete job means these records are missing from the backup.
	RecsReadFailed uint64
	// DrainBlockedMigrations is only reported while the job drains and
	// migrations hold it back.
	DrainBlockedMigrations uint64
	// CountReadFailures counts read failures of the base count.
	CountReadFailures uint64

	// PartitionsFlushed counts partitions whose data is durable in storage.
	PartitionsFlushed uint32
	// PartitionsOwned counts partitions the node is responsible for.
	PartitionsOwned uint32
	// PartitionsScanned counts partitions already scanned.
	PartitionsScanned uint32
	// PartitionsCountPending counts partitions not complete yet.
	PartitionsCountPending uint32

	// ProgressPct is 0-50 for the base scan, 50-95 for the incremental scan,
	// holds at 95 while draining and is 100 only once the job is complete.
	ProgressPct        float64
	ChangeStreamActive bool

	// BackpressureActive is true while the scan is throttled by the upload pipeline.
	BackpressureActive bool
	// BackpressureActivations counts the times the backpressure engaged.
	BackpressureActivations uint64
	// BytesInPerSec is the ingest rate over the last ~3 seconds.
	BytesInPerSec uint64
	// BytesOutPerSec is the upload rate over the last ~3 seconds.
	BytesOutPerSec uint64
	// UploadQDepth is the current depth of the upload queue.
	UploadQDepth uint64
	// UploadQCap is the capacity of the upload queue.
	UploadQCap uint64
	// BuffersInUse is the number of buffers in use.
	BuffersInUse uint64
	// TotalBuffersAllocated is the number of buffers allocated.
	TotalBuffersAllocated uint64
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
			AbortOrigin: AbortOrigin(r[fieldAbortOrigin]),
			StartTime:   parseInfoTime(r[fieldStartTime]),
			FinishTime:  parseInfoTime(r[fieldFinishTime]),
		}

		status.parseFields(r)

		return status
	}

	return nil
}

// parseFields fills the numeric and boolean fields from the info map, see
// [parseInfoFields] for the fields that are missing or unparsable.
func (s *ResponseBackupState) parseFields(r InfoMap) {
	parseInfoFields(r, InfoMap.ParseUint64,
		infoFieldOf(fieldRecsBackedUp, &s.RecsBackedUp),
		infoFieldOf(fieldRecsBase, &s.RecsBase),
		infoFieldOf(fieldRecsQueryIncr, &s.RecsQueryIncr),
		infoFieldOf(fieldRecsChangeIncr, &s.RecsChangeIncr),
		infoFieldOf(fieldBytesBackedUp, &s.BytesBackedUp),
		infoFieldOf(fieldBytesBase, &s.BytesBase),
		infoFieldOf(fieldBytesQueryIncr, &s.BytesQueryIncr),
		infoFieldOf(fieldBytesChangeIncr, &s.BytesChangeIncr),
		infoFieldOf(fieldRecsFiltered, &s.RecsFiltered),
		infoFieldOf(fieldRecsDegenerate, &s.RecsDegenerate),
		infoFieldOf(fieldRecsSkippedXDRTomb, &s.RecsSkippedXDRTomb),
		infoFieldOf(fieldRecsReadFailed, &s.RecsReadFailed),
		infoFieldOf(fieldDrainBlockedMigrations, &s.DrainBlockedMigrations),
		infoFieldOf(fieldCountReadFailures, &s.CountReadFailures),
		infoFieldOf(fieldBackpressureActivations, &s.BackpressureActivations),
		infoFieldOf(fieldBytesInPerSec, &s.BytesInPerSec),
		infoFieldOf(fieldBytesOutPerSec, &s.BytesOutPerSec),
		infoFieldOf(fieldUploadQDepth, &s.UploadQDepth),
		infoFieldOf(fieldUploadQCap, &s.UploadQCap),
		infoFieldOf(fieldBuffersInUse, &s.BuffersInUse),
		infoFieldOf(fieldTotalBuffersAllocated, &s.TotalBuffersAllocated),
	)

	parseInfoFields(r, InfoMap.ParseUint32,
		infoFieldOf(fieldBackupRPS, &s.BackupRPS),
		infoFieldOf(fieldBackupWorkerThreads, &s.BackupWorkerThreads),
		infoFieldOf(fieldBackupUploadThreads, &s.BackupUploadThreads),
		infoFieldOf(fieldBackupS3ThroughputTargetGbps, &s.BackupS3ThroughputTargetGbps),
		infoFieldOf(fieldPartitionsFlushed, &s.PartitionsFlushed),
		infoFieldOf(fieldPartitionsOwned, &s.PartitionsOwned),
		infoFieldOf(fieldPartitionsScanned, &s.PartitionsScanned),
		infoFieldOf(fieldPartitionsCountPending, &s.PartitionsCountPending),
	)

	parseInfoFields(r, InfoMap.ParseFloat64,
		infoFieldOf(fieldProgressPct, &s.ProgressPct),
	)

	parseInfoFields(r, InfoMap.ParseBool,
		infoFieldOf(fieldChangeStreamActive, &s.ChangeStreamActive),
		infoFieldOf(fieldBackpressureActive, &s.BackpressureActive),
	)
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

// MergeResponseBackupStates combines per-node backup status responses into a
// single cluster-wide view. Counters and gauges are summed, the effective
// limits, the same on every node, are taken as their maximum, StartTime is the
// earliest timestamp across nodes, FinishTime is the latest, and ErrorReason
// and AbortOrigin are the first ones reported by any node.
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

		merged.add(status)

		if status.PartitionsOwned > 0 {
			progressWeightedSum += status.ProgressPct * float64(status.PartitionsOwned)
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

// add merges the figures of a single node into s, see [MergeResponseBackupStates].
func (s *ResponseBackupState) add(n *ResponseBackupState) {
	s.ErrorReason = firstSet(s.ErrorReason, n.ErrorReason)
	s.AbortOrigin = firstSet(s.AbortOrigin, n.AbortOrigin)
	s.StartTime = earliestTime(s.StartTime, n.StartTime)
	s.FinishTime = latestTime(s.FinishTime, n.FinishTime)

	s.BackupRPS = max(s.BackupRPS, n.BackupRPS)
	s.BackupWorkerThreads = max(s.BackupWorkerThreads, n.BackupWorkerThreads)
	s.BackupUploadThreads = max(s.BackupUploadThreads, n.BackupUploadThreads)
	s.BackupS3ThroughputTargetGbps = max(s.BackupS3ThroughputTargetGbps, n.BackupS3ThroughputTargetGbps)

	s.RecsBackedUp += n.RecsBackedUp
	s.RecsBase += n.RecsBase
	s.RecsQueryIncr += n.RecsQueryIncr
	s.RecsChangeIncr += n.RecsChangeIncr
	s.BytesBackedUp += n.BytesBackedUp
	s.BytesBase += n.BytesBase
	s.BytesQueryIncr += n.BytesQueryIncr
	s.BytesChangeIncr += n.BytesChangeIncr
	s.RecsFiltered += n.RecsFiltered
	s.RecsDegenerate += n.RecsDegenerate
	s.RecsSkippedXDRTomb += n.RecsSkippedXDRTomb
	s.RecsReadFailed += n.RecsReadFailed
	s.DrainBlockedMigrations += n.DrainBlockedMigrations
	s.CountReadFailures += n.CountReadFailures

	s.PartitionsFlushed += n.PartitionsFlushed
	s.PartitionsOwned += n.PartitionsOwned
	s.PartitionsScanned += n.PartitionsScanned
	s.PartitionsCountPending += n.PartitionsCountPending

	s.ChangeStreamActive = s.ChangeStreamActive || n.ChangeStreamActive
	s.BackpressureActive = s.BackpressureActive || n.BackpressureActive
	s.BackpressureActivations += n.BackpressureActivations
	s.BytesInPerSec += n.BytesInPerSec
	s.BytesOutPerSec += n.BytesOutPerSec
	s.UploadQDepth += n.UploadQDepth
	s.UploadQCap += n.UploadQCap
	s.BuffersInUse += n.BuffersInUse
	s.TotalBuffersAllocated += n.TotalBuffersAllocated
}
