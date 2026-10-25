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

// BackupStatus is the commit verdict of a backup. A backup without
// metadata.json has not committed, and restore refuses it. A record without a
// status field is read by the server as an empty status.
type BackupStatus string

const (
	// BackupStatusComplete means the backup committed; restore may use it.
	BackupStatusComplete BackupStatus = "complete"
	// BackupStatusAborted means the principal aborted the backup and left this
	// record as a tombstone; restore refuses it.
	BackupStatusAborted BackupStatus = "aborted"
)

// BackupType tells a full backup from an incremental one. It follows from the
// modified-after and modified-before bounds alone.
type BackupType string

const (
	// BackupTypeFull means neither bound was set.
	BackupTypeFull BackupType = "full"
	// BackupTypeIncremental means at least one bound was set.
	BackupTypeIncremental BackupType = "incremental"
)

// BackupPolicy is the consistency the operator asked for.
type BackupPolicy string

const (
	// BackupPolicyStrict means the backup reaches a strict cluster-wide
	// checkpoint or fails.
	BackupPolicyStrict BackupPolicy = "strict"
	// BackupPolicyBestEffort means the backup tries strict and falls back to
	// fuzzy on a node failure; see Metadata.FallbackReason.
	BackupPolicyBestEffort BackupPolicy = "best-effort"
	// BackupPolicyFuzzy means a base scan only: no change stream and no
	// checkpoint.
	BackupPolicyFuzzy BackupPolicy = "fuzzy"
)

// AchievedConsistency is the consistency a complete backup actually reached.
type AchievedConsistency string

const (
	// AchievedConsistencyStrict means the incremental phase completed and
	// Metadata.CheckpointMs is the restore cut. A record without the field is
	// treated as strict too.
	AchievedConsistencyStrict AchievedConsistency = "strict"
	// AchievedConsistencyFuzzy means a base scan only, by policy or after a
	// best-effort fall-back. Metadata.CheckpointMs is 0, and restore applies
	// every record the backup holds.
	AchievedConsistencyFuzzy AchievedConsistency = "fuzzy"
)

// Metadata is the backup's commit record (metadata.json), written only by the
// principal node. It is a status marker: per-partition data lives in manifests.
type Metadata struct {
	// Backup identifier (the job id); also the object-store folder name.
	// Always present.
	BackupID string `json:"backup_id"`

	// Aerospike namespace the backup was taken from. Always present.
	Namespace string `json:"namespace"`

	// Set filter exactly as the operator gave it (comma-separated set names).
	// Omitted when the backup covers all sets.
	Sets string `json:"sets,omitempty"`

	// Commit verdict. Always present. See BackupStatus.
	Status BackupStatus `json:"status"`

	// Strict checkpoint - the cluster-wide restore cut. RAW Citrusleaf epoch
	// milliseconds (ms since 2010-01-01T00:00:00Z, same units as record
	// last-update-time), NOT Unix time. Always present: 0 for a fuzzy backup,
	// for an aborted tombstone, or from pre-feature builds (restore then applies
	// the change lane unfiltered).
	CheckpointMs int64 `json:"checkpoint_ms"`

	// "full" if neither modified bound is set, otherwise "incremental".
	// Always present (missing only in objects from older builds).
	BackupType BackupType `json:"backup_type"`

	// Lower bound of record last-update-time as wall-clock UTC ISO8601 with
	// milliseconds, e.g. "2026-10-25T12:34:56.000Z". Omitted if unset.
	ModifiedAfter string `json:"modified_after,omitempty"`
	// Same bound as Unix seconds (wall clock). Omitted if unset. Restore reads
	// this field, not the ISO one.
	ModifiedAfterUnixS int64 `json:"modified_after_unix_s,omitempty"`

	// Upper bound of record last-update-time, ISO8601 UTC. Omitted if unset.
	ModifiedBefore string `json:"modified_before,omitempty"`
	// Same bound as Unix seconds (wall clock). Omitted if unset.
	ModifiedBeforeUnixS int64 `json:"modified_before_unix_s,omitempty"`

	// FNV-1a 64-bit hash of what the backup selected (set-list, bin-list,
	// filter-exp; names order-insensitive), as a 16-char lowercase hex string,
	// e.g. "a1b2c3d4e5f60718". Lets restore check that all backups in a
	// backup-ids chain share one scope. Omitted when the hash is 0 (never
	// computed).
	Scope string `json:"scope,omitempty"`

	// Policy the operator requested. Present on both complete and aborted
	// records from current builds; omitted from older ones.
	BackupPolicy BackupPolicy `json:"backup_policy,omitempty"`

	// What a complete backup achieved. Written only for status "complete";
	// absent on aborted tombstones and from older builds (absent = strict).
	AchievedConsistency AchievedConsistency `json:"achieved_consistency,omitempty"`

	// Why a best-effort backup fell back to fuzzy. Free text, typically
	// "node <hex-node-id> failed - <reason>" (or the re-adopted reason verbatim
	// after a principal failover). Only on a complete best-effort fall-back.
	FallbackReason string `json:"fallback_reason,omitempty"`

	// Fuzzy only: when the base scan became durable. RAW Citrusleaf epoch ms,
	// same units as CheckpointMs. Omitted when 0 (strict backups, aborted).
	BaseScanEndMs int64 `json:"base_scan_end_ms,omitempty"`
}
