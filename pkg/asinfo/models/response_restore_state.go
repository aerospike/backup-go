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
	"cmp"
	"time"
)

// Keys of the "restore-status" info response, besides the shared ones of
// job_status.go.
const (
	fieldKind                   = "kind"
	fieldRestoreRPS             = "restore-rps"
	fieldRestoreWorkerThreads   = "restore-worker-threads"
	fieldRestoreDownloadThreads = "restore-download-threads"
	fieldNsState                = "ns-state"

	fieldJoiner             = "joiner"
	fieldPlaceholder        = "placeholder"
	fieldPidsInScope        = "pids-in-scope"
	fieldMissingLayoutNodes = "missing-layout-nodes"
	fieldNDoneNodes         = "n-done-nodes"
	fieldBackupID           = "backup-id"
	fieldBackupEntry        = "backup-entry"
	fieldBackupEntries      = "backup-entries"
	fieldExtraTTL           = "extra-ttl"
	fieldTTLExtended        = "ttl-extended"
	fieldTTLClamped         = "ttl-clamped"
	fieldClusterKey         = "cluster-key"
	fieldDone               = "done"
	fieldClear              = "clear"
	fieldClearZeroedBytes   = "clear-zeroed-bytes"
	fieldClearMs            = "clear-ms"
	fieldHeldReason         = "held-reason"
	fieldPendingQDepth      = "pending-q-depth"
	fieldPendingQCap        = "pending-q-cap"
	fieldSourceConsistency  = "source-consistency"

	fieldTotal           = "total"
	fieldApplied         = "applied"
	fieldDeletes         = "deletes"
	fieldFiltered        = "filtered"
	fieldFilteredNotOurs = "filtered-not-ours"
	fieldFailed          = "failed"

	fieldOutstanding       = "outstanding"
	fieldMaxInflight       = "max-inflight"
	fieldIgnoreRecordError = "ignore-record-error"
	fieldCovered           = "covered"
	fieldClusterWide       = "cluster-wide"
	fieldDeleteAbsent      = "delete-absent"
)

// ResponseRestoreState represents the status of a server-side restore job, as
// reported by the "restore-status" info command.
type ResponseRestoreState struct {
	// StartTime is zero while the job has not started yet.
	StartTime time.Time
	// FinishTime is zero while the job has not finished yet.
	FinishTime time.Time

	JobID     string
	Namespace string
	Kind      RestoreKind
	State     RestoreState
	// NsState is the restore gate of the namespace. It is NamespaceStateStub
	// on a stub row.
	NsState NamespaceState
	// ErrorReason is only reported by the server when the job failed or was aborted.
	ErrorReason string
	// AbortOrigin is only reported by the server when the job is aborting or aborted.
	AbortOrigin AbortOrigin

	// RestoreRPS is the effective records per second limit, 0 for no limit.
	// A node that restarted and holds the gate from its local restore file
	// (Placeholder) reports 0 for it and for the thread counts.
	RestoreRPS uint32
	// RestoreWorkerThreads is the effective number of apply threads per node.
	RestoreWorkerThreads uint32
	// RestoreDownloadThreads is the effective number of download threads per node.
	RestoreDownloadThreads uint32

	// BackupID is the backup the node is applying, or applied last. It is only
	// reported for more than one backup id.
	BackupID string
	// BackupEntry is the 0-based index of BackupID in the backup ids of the
	// request. A cold restore counts it down to 0, the full backup; a fuzzy
	// restore counts it up from 0.
	BackupEntry uint32
	// BackupEntries is the number of backup ids of the request. It is only
	// reported for more than one backup id.
	BackupEntries uint32

	// ExtraTTL is the effective extra TTL. It and the two TTL counters are only
	// reported when the extra TTL is not zero.
	ExtraTTL uint32
	// TTLExtended counts records whose expiry was moved forward.
	TTLExtended uint64
	// TTLClamped counts the records of TTLExtended capped at now + 10 years.
	TTLClamped uint64

	// SourceConsistency is the lowest consistency of the backups the node read
	// so far. It is empty before the node read any.
	SourceConsistency Consistency
	// RecsPerSec is the rate of Applied, as of the last lap of the monitor.
	RecsPerSec uint64
	// LastLapMs is how long ago the restore monitor last reached the job, in
	// milliseconds. A growing value shows a monitor blocked on the object
	// store. It is nil for a finished job, or before the first lap.
	LastLapMs *uint64

	// ProgressPct is the progress of the job, from 0 to 100. A node reports it
	// for its own share of the partitions, and reports 100 only once the job
	// is complete. See [MergeResponseRestoreStates] for the cluster-wide value.
	ProgressPct float64

	// Record counters, reported by both modes. A record read lands in one of
	// the buckets Applied, Filtered and Failed once its outcome is final;
	// Deletes and FilteredNotOurs are parts of Applied and Filtered.

	// Total is the sum of Applied, Filtered and Failed. It can fall short of
	// the records read: records in flight or abandoned on a stop are in no
	// bucket.
	Total uint64
	// Applied counts the records written, durable deletes included.
	Applied uint64
	// Deletes counts the durable deletes of Applied.
	Deletes uint64
	// Filtered counts the records read but not written: rejected by the filter
	// expression, out of the scope of the node, or skipped otherwise.
	Filtered uint64
	// FilteredNotOurs counts the records of Filtered dropped because another
	// node owns their partition. Only a cold restore reports it.
	FilteredNotOurs uint64
	// Failed counts the records that failed, the ones a fuzzy restore
	// tolerates with IgnoreRecordError included.
	Failed uint64

	// Cold restore fields.

	// Joiner is true if the node joined after the layout was frozen.
	Joiner bool
	// Placeholder is true if the node restarted and holds the gate from its
	// local restore file while it waits for the cluster metadata.
	Placeholder bool
	// PidsInScope counts the partitions the node hydrates: those of the
	// restore layout it holds a hydrated copy of. Merged across the nodes, it
	// counts each partition once per hydrated copy.
	PidsInScope uint32
	// MissingLayoutNodes counts the layout nodes not in the cluster. A non-zero
	// value blocks the completion.
	MissingLayoutNodes uint32
	// NDoneNodes counts the layout nodes that finished their share.
	NDoneNodes uint32
	// ClusterKey is the cluster key, in hex, the job is running under.
	ClusterKey string
	// Done is true when the job is complete. While the job is live, only the
	// principal sets it.
	Done bool
	// Clear is the storage clear state of the node. A node reporting
	// ClearStateRequired must be cleared with restore-clear-storage before its
	// namespace can reach READY.
	Clear ClearState
	// ClearZeroedBytes counts the bytes zeroed by the last completed clear.
	ClearZeroedBytes uint64
	// ClearMs is the wall time of the last completed clear, in milliseconds.
	ClearMs uint64
	// PendingQDepth is the depth of the download queue of the node, as of the
	// last lap of the monitor.
	PendingQDepth uint64
	// PendingQCap is the capacity of the download queue of the node.
	PendingQCap uint64
	// HeldReason tells why a live job holds the node in PREPARING, such as
	// "truncate-state-live" for a namespace with truncate state to undo. It is
	// only reported while the node is held.
	HeldReason string

	// Fuzzy restore fields. With more than one backup id, the coverage counters
	// are for the entry of BackupEntry.

	// Outstanding counts writes in service or waiting for a retry.
	Outstanding uint64
	// MaxInflight is the ceiling of Outstanding the writes are throttled at.
	MaxInflight uint32
	// IgnoreRecordError tells whether the records of Failed are tolerated
	// instead of failing the job.
	IgnoreRecordError bool
	// Covered counts partitions the node has fully restored.
	Covered uint32
	// ClusterWide counts partitions known to be covered across the cluster.
	ClusterWide uint32
	// DeleteAbsent counts the deletes of Deletes whose record was already
	// absent. It is only reported for more than one backup id.
	DeleteAbsent uint64
}

// NewResponseRestoreState builds a ResponseRestoreState from the first InfoMap
// entry that contains a state field.
func NewResponseRestoreState(im []InfoMap) *ResponseRestoreState {
	for _, r := range im {
		state, ok := r[fieldState]
		if !ok {
			continue
		}

		status := &ResponseRestoreState{
			JobID:       r[fieldJobID],
			Namespace:   r[fieldNamespace],
			Kind:        RestoreKind(r[fieldKind]),
			State:       RestoreState(state),
			NsState:     NamespaceState(r[fieldNsState]),
			ErrorReason: r[fieldErrorReason],
			AbortOrigin: AbortOrigin(r[fieldAbortOrigin]),
			BackupID:    r[fieldBackupID],
			ClusterKey:  r[fieldClusterKey],
			Clear:       ClearState(r[fieldClear]),
			HeldReason:  infoValue(r, fieldHeldReason),
			LastLapMs:   parseOptionalUint64(r, fieldLastLapMs),

			SourceConsistency: Consistency(infoValue(r, fieldSourceConsistency)),
			StartTime:         parseInfoTime(r[fieldStartTime]),
			FinishTime:        parseInfoTime(r[fieldFinishTime]),
		}

		status.parseFields(r)

		return status
	}

	return nil
}

// parseFields fills the numeric and boolean fields from the info map, see
// [parseInfoFields] for the fields that are missing or unparsable.
func (s *ResponseRestoreState) parseFields(r InfoMap) {
	parseInfoFields(r, InfoMap.ParseUint64,
		infoFieldOf(fieldTTLExtended, &s.TTLExtended),
		infoFieldOf(fieldTTLClamped, &s.TTLClamped),
		infoFieldOf(fieldTotal, &s.Total),
		infoFieldOf(fieldApplied, &s.Applied),
		infoFieldOf(fieldDeletes, &s.Deletes),
		infoFieldOf(fieldFiltered, &s.Filtered),
		infoFieldOf(fieldFilteredNotOurs, &s.FilteredNotOurs),
		infoFieldOf(fieldFailed, &s.Failed),
		infoFieldOf(fieldOutstanding, &s.Outstanding),
		infoFieldOf(fieldRecsPerSec, &s.RecsPerSec),
		infoFieldOf(fieldPendingQDepth, &s.PendingQDepth),
		infoFieldOf(fieldPendingQCap, &s.PendingQCap),
		infoFieldOf(fieldDeleteAbsent, &s.DeleteAbsent),
		infoFieldOf(fieldClearZeroedBytes, &s.ClearZeroedBytes),
		infoFieldOf(fieldClearMs, &s.ClearMs),
	)

	parseInfoFields(r, InfoMap.ParseUint32,
		infoFieldOf(fieldRestoreRPS, &s.RestoreRPS),
		infoFieldOf(fieldRestoreWorkerThreads, &s.RestoreWorkerThreads),
		infoFieldOf(fieldRestoreDownloadThreads, &s.RestoreDownloadThreads),
		infoFieldOf(fieldBackupEntry, &s.BackupEntry),
		infoFieldOf(fieldBackupEntries, &s.BackupEntries),
		infoFieldOf(fieldExtraTTL, &s.ExtraTTL),
		infoFieldOf(fieldPidsInScope, &s.PidsInScope),
		infoFieldOf(fieldMissingLayoutNodes, &s.MissingLayoutNodes),
		infoFieldOf(fieldNDoneNodes, &s.NDoneNodes),
		infoFieldOf(fieldCovered, &s.Covered),
		infoFieldOf(fieldClusterWide, &s.ClusterWide),
		infoFieldOf(fieldMaxInflight, &s.MaxInflight),
	)

	parseInfoFields(r, InfoMap.ParseFloat64,
		infoFieldOf(fieldProgressPct, &s.ProgressPct),
	)

	parseInfoFields(r, InfoMap.ParseBool,
		infoFieldOf(fieldJoiner, &s.Joiner),
		infoFieldOf(fieldPlaceholder, &s.Placeholder),
		infoFieldOf(fieldDone, &s.Done),
		infoFieldOf(fieldIgnoreRecordError, &s.IgnoreRecordError),
	)
}

// MergeResponseRestoreStates combines per-node restore status responses into a
// single cluster-wide view:
//   - State, NsState, Clear and SourceConsistency are resolved with
//     [ResolveRestoreState], [ResolveNamespaceState], [ResolveClearState] and
//     [ResolveConsistency];
//   - the per-node counters, rates and gauges are summed, PidsInScope and
//     ClearZeroedBytes included;
//   - the effective limits, MaxInflight, ClearMs and the figures every node
//     reports in the cluster view (MissingLayoutNodes, NDoneNodes, ClusterWide,
//     BackupEntries) are taken as their maximum, and LastLapMs is the oldest
//     lap;
//   - BackupID and BackupEntry are those of the node that is the furthest
//     behind;
//   - Joiner, Placeholder, Done and IgnoreRecordError are true if they are
//     true on any node;
//   - ProgressPct is the mean of the progress of the nodes weighted by their
//     share of the partitions, see [ResponseRestoreState.progressWeight];
//   - StartTime is the earliest timestamp, FinishTime the latest, or zero
//     while any node has not finished, JobID and Namespace are the first ones
//     reported by a row that is not a stub, and the other strings are the
//     first ones reported by any node.
func MergeResponseRestoreStates(statuses []*ResponseRestoreState) *ResponseRestoreState {
	var (
		merged      *ResponseRestoreState
		states      []RestoreState
		nsStates    []NamespaceState
		clearStates []ClearState
		sources     []Consistency
		hasBackup   bool
		unfinished  bool

		progressWeightedSum float64
		progressWeight      float64
	)

	for _, status := range statuses {
		if status == nil {
			continue
		}

		if merged == nil {
			merged = &ResponseRestoreState{Kind: status.Kind}
		}

		unfinished = unfinished || status.FinishTime.IsZero()

		states = append(states, status.State)
		nsStates = append(nsStates, status.NsState)
		clearStates = append(clearStates, status.Clear)
		sources = append(sources, status.SourceConsistency)

		if status.BackupEntries > 0 && (!hasBackup || merged.isBehind(status)) {
			merged.BackupID = status.BackupID
			merged.BackupEntry = status.BackupEntry
			hasBackup = true
		}

		merged.add(status)

		weight := status.progressWeight()
		progressWeightedSum += status.ProgressPct * weight
		progressWeight += weight
	}

	if merged == nil {
		return nil
	}

	// Each node reports its own progress-pct for its own partitions. Those
	// percentages aren't equally sized slices of the cluster job unless every
	// node has the same number of partitions.
	if progressWeight > 0 {
		merged.ProgressPct = progressWeightedSum / progressWeight
	}

	if unfinished {
		merged.FinishTime = time.Time{}
	}

	merged.State = ResolveRestoreState(states)
	merged.NsState = ResolveNamespaceState(nsStates)
	merged.Clear = ResolveClearState(clearStates)
	merged.SourceConsistency = ResolveConsistency(sources)

	return merged
}

// IsFinished reports whether the job is terminal on the node: COMPLETE,
// FAILED or ABORTED.
func (s *ResponseRestoreState) IsFinished() bool {
	return isRestoreStateTerminal(s.State)
}

// HasNamespace reports whether the row names its namespace. A stub row, a job
// that never got its parameters, may report "(none)" or "-" in place of it.
func (s *ResponseRestoreState) HasNamespace() bool {
	return knownName(s.Namespace)
}

// progressWeight returns the share of the partitions the ProgressPct of node s
// is measured against. A cold restore reports it as PidsInScope; a node with
// no partitions in scope takes no part in the progress. A fuzzy restore
// measures the progress against the partitions the node is master of, which it
// does not report: they are spread evenly across the nodes, so every node
// weighs the same.
func (s *ResponseRestoreState) progressWeight() float64 {
	if s.Kind == RestoreKindCold {
		return float64(s.PidsInScope)
	}

	return 1
}

// isBehind reports whether node n applies an earlier backup entry than s: a
// cold restore walks the backup ids newest first, a fuzzy restore oldest first.
func (s *ResponseRestoreState) isBehind(n *ResponseRestoreState) bool {
	if s.Kind == RestoreKindCold {
		return n.BackupEntry > s.BackupEntry
	}

	return n.BackupEntry < s.BackupEntry
}

// add merges the figures of a single node into s, see [MergeResponseRestoreStates].
func (s *ResponseRestoreState) add(n *ResponseRestoreState) {
	s.JobID = firstKnownName(s.JobID, n.JobID)
	s.Namespace = firstKnownName(s.Namespace, n.Namespace)
	s.ErrorReason = cmp.Or(s.ErrorReason, n.ErrorReason)
	s.AbortOrigin = cmp.Or(s.AbortOrigin, n.AbortOrigin)
	s.ClusterKey = cmp.Or(s.ClusterKey, n.ClusterKey)
	s.HeldReason = cmp.Or(s.HeldReason, n.HeldReason)
	s.StartTime = earliestTime(s.StartTime, n.StartTime)
	s.FinishTime = latestTime(s.FinishTime, n.FinishTime)
	s.LastLapMs = maxOptional(s.LastLapMs, n.LastLapMs)

	s.RestoreRPS = max(s.RestoreRPS, n.RestoreRPS)
	s.RestoreWorkerThreads = max(s.RestoreWorkerThreads, n.RestoreWorkerThreads)
	s.RestoreDownloadThreads = max(s.RestoreDownloadThreads, n.RestoreDownloadThreads)
	s.ExtraTTL = max(s.ExtraTTL, n.ExtraTTL)
	s.BackupEntries = max(s.BackupEntries, n.BackupEntries)
	s.MissingLayoutNodes = max(s.MissingLayoutNodes, n.MissingLayoutNodes)
	s.NDoneNodes = max(s.NDoneNodes, n.NDoneNodes)
	s.ClusterWide = max(s.ClusterWide, n.ClusterWide)
	s.ClearMs = max(s.ClearMs, n.ClearMs)
	s.MaxInflight = max(s.MaxInflight, n.MaxInflight)

	s.Joiner = s.Joiner || n.Joiner
	s.Placeholder = s.Placeholder || n.Placeholder
	s.Done = s.Done || n.Done
	s.IgnoreRecordError = s.IgnoreRecordError || n.IgnoreRecordError

	s.TTLExtended += n.TTLExtended
	s.TTLClamped += n.TTLClamped
	s.Total += n.Total
	s.Applied += n.Applied
	s.Deletes += n.Deletes
	s.Filtered += n.Filtered
	s.FilteredNotOurs += n.FilteredNotOurs
	s.Failed += n.Failed
	s.Outstanding += n.Outstanding
	s.RecsPerSec += n.RecsPerSec
	s.PendingQDepth += n.PendingQDepth
	s.PendingQCap += n.PendingQCap
	s.Covered += n.Covered
	s.DeleteAbsent += n.DeleteAbsent
	s.PidsInScope += n.PidsInScope
	s.ClearZeroedBytes += n.ClearZeroedBytes
}
