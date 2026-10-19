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

	fieldApplied      = "applied"
	fieldIgnored      = "ignored"
	fieldFailed       = "failed"
	fieldOutstanding  = "outstanding"
	fieldSkipped      = "skipped"
	fieldHandedOff    = "handed-off"
	fieldCovered      = "covered"
	fieldClusterWide  = "cluster-wide"
	fieldRead         = "read"
	fieldDeleted      = "deleted"
	fieldDeleteAbsent = "delete-absent"
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
	// NsState is the restore gate of the namespace. It is NamespaceStateNotHosted
	// on a node that does not host the namespace.
	NsState NamespaceState
	// ErrorReason is only reported by the server when the job failed or was aborted.
	ErrorReason string
	// AbortOrigin is only reported by the server when the job is aborting or aborted.
	AbortOrigin AbortOrigin

	// RestoreRPS is the effective records per second limit, 0 for no limit.
	// A cold job that is prepared but not started yet reports 0 for it and for
	// the thread counts.
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

	// Cold restore fields.

	// Joiner is true if the node joined after the layout was frozen.
	Joiner bool
	// Placeholder is true if the node restarted and holds the gate from its
	// local restore file while it waits for the cluster metadata.
	Placeholder bool
	// PidsInScope counts the partitions in the restore layout.
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

	// Fuzzy restore fields. With more than one backup id, the coverage counters
	// are for the entry of BackupEntry.

	// Applied counts records written.
	Applied uint64
	// Ignored counts records that failed but were tolerated.
	Ignored uint64
	// Failed counts records that failed.
	Failed uint64
	// Outstanding counts writes in service or waiting for a retry.
	Outstanding uint64
	// Skipped counts records not replayed: filtered, expired or tombstones.
	Skipped uint64
	// HandedOff counts records skipped because their partition moved to another node.
	HandedOff uint64
	// Covered counts partitions the node has fully restored.
	Covered uint32
	// ClusterWide counts partitions known to be covered across the cluster.
	ClusterWide uint32
	// Read counts records read from the backup.
	Read uint64
	// Deleted counts durable deletes replayed from the incremental backups.
	Deleted uint64
	// DeleteAbsent counts the deletes of Deleted whose record was already absent.
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
func (s *ResponseRestoreState) parseFields(r InfoMap) {
	parseInfoFields(r, InfoMap.ParseUint64,
		infoFieldOf(fieldTTLExtended, &s.TTLExtended),
		infoFieldOf(fieldTTLClamped, &s.TTLClamped),
		infoFieldOf(fieldApplied, &s.Applied),
		infoFieldOf(fieldIgnored, &s.Ignored),
		infoFieldOf(fieldFailed, &s.Failed),
		infoFieldOf(fieldOutstanding, &s.Outstanding),
		infoFieldOf(fieldSkipped, &s.Skipped),
		infoFieldOf(fieldHandedOff, &s.HandedOff),
		infoFieldOf(fieldRead, &s.Read),
		infoFieldOf(fieldDeleted, &s.Deleted),
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
	)

	parseInfoFields(r, InfoMap.ParseBool,
		infoFieldOf(fieldJoiner, &s.Joiner),
		infoFieldOf(fieldPlaceholder, &s.Placeholder),
		infoFieldOf(fieldDone, &s.Done),
	)
}

// MergeResponseRestoreStates combines per-node restore status responses into a
// single cluster-wide view:
//   - State, NsState and Clear are resolved with [ResolveRestoreState],
//     [ResolveNamespaceState] and [ResolveClearState];
//   - the per-node counters are summed, ClearZeroedBytes included;
//   - the effective limits, ClearMs and the figures every node reports in the
//     cluster view (PidsInScope, MissingLayoutNodes, NDoneNodes, ClusterWide,
//     BackupEntries) are taken as their maximum;
//   - BackupID and BackupEntry are those of the node that is the furthest
//     behind;
//   - Joiner, Placeholder and Done are true if they are true on any node;
//   - StartTime is the earliest timestamp, FinishTime the latest, and the
//     strings are the first ones reported by any node.
func MergeResponseRestoreStates(statuses []*ResponseRestoreState) *ResponseRestoreState {
	var (
		merged      *ResponseRestoreState
		states      []RestoreState
		nsStates    []NamespaceState
		clearStates []ClearState
		hasBackup   bool
	)

	for _, status := range statuses {
		if status == nil {
			continue
		}

		if merged == nil {
			merged = &ResponseRestoreState{
				JobID:     status.JobID,
				Namespace: status.Namespace,
				Kind:      status.Kind,
			}
		}

		states = append(states, status.State)
		nsStates = append(nsStates, status.NsState)
		clearStates = append(clearStates, status.Clear)

		if status.BackupEntries > 0 && (!hasBackup || merged.isBehind(status)) {
			merged.BackupID = status.BackupID
			merged.BackupEntry = status.BackupEntry
			hasBackup = true
		}

		merged.add(status)
	}

	if merged == nil {
		return nil
	}

	merged.State = ResolveRestoreState(states)
	merged.NsState = ResolveNamespaceState(nsStates)
	merged.Clear = ResolveClearState(clearStates)

	return merged
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
	s.ErrorReason = cmp.Or(s.ErrorReason, n.ErrorReason)
	s.AbortOrigin = cmp.Or(s.AbortOrigin, n.AbortOrigin)
	s.ClusterKey = cmp.Or(s.ClusterKey, n.ClusterKey)
	s.StartTime = earliestTime(s.StartTime, n.StartTime)
	s.FinishTime = latestTime(s.FinishTime, n.FinishTime)

	s.RestoreRPS = max(s.RestoreRPS, n.RestoreRPS)
	s.RestoreWorkerThreads = max(s.RestoreWorkerThreads, n.RestoreWorkerThreads)
	s.RestoreDownloadThreads = max(s.RestoreDownloadThreads, n.RestoreDownloadThreads)
	s.ExtraTTL = max(s.ExtraTTL, n.ExtraTTL)
	s.BackupEntries = max(s.BackupEntries, n.BackupEntries)
	s.PidsInScope = max(s.PidsInScope, n.PidsInScope)
	s.MissingLayoutNodes = max(s.MissingLayoutNodes, n.MissingLayoutNodes)
	s.NDoneNodes = max(s.NDoneNodes, n.NDoneNodes)
	s.ClusterWide = max(s.ClusterWide, n.ClusterWide)
	s.ClearMs = max(s.ClearMs, n.ClearMs)

	s.Joiner = s.Joiner || n.Joiner
	s.Placeholder = s.Placeholder || n.Placeholder
	s.Done = s.Done || n.Done

	s.TTLExtended += n.TTLExtended
	s.TTLClamped += n.TTLClamped
	s.Applied += n.Applied
	s.Ignored += n.Ignored
	s.Failed += n.Failed
	s.Outstanding += n.Outstanding
	s.Skipped += n.Skipped
	s.HandedOff += n.HandedOff
	s.Covered += n.Covered
	s.Read += n.Read
	s.Deleted += n.Deleted
	s.DeleteAbsent += n.DeleteAbsent
	s.ClearZeroedBytes += n.ClearZeroedBytes
}

// IsStarted reports whether the status belongs to a restore job that was
// started with the restore command, and not only prepared. A fuzzy job exists
// only once it was started. A prepared cold job is reported before its start,
// with zero effective limits and its namespace not yet RESTORING.
func (s *ResponseRestoreState) IsStarted() bool {
	if s.Kind == RestoreKindFuzzy {
		return true
	}

	return s.RestoreWorkerThreads > 0 ||
		s.NsState == NamespaceStateRestoring ||
		s.NsState == NamespaceStateOpening
}
