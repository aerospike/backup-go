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

// BackupPolicy is how consistent a backup must be, see [RequestBackup.Policy].
type BackupPolicy string

// Backup policies the server accepts.
const (
	// BackupPolicyStrict aborts the backup on any failure.
	BackupPolicyStrict BackupPolicy = "strict"
	// BackupPolicyBestEffort tries a strict backup. A failure after the base
	// scan is durable cluster-wide commits the backup as fuzzy instead of
	// aborting it.
	BackupPolicyBestEffort BackupPolicy = "best-effort"
	// BackupPolicyFuzzy runs the base scan only, without the change stream.
	BackupPolicyFuzzy BackupPolicy = "fuzzy"
)

// RestorePolicy selects the restore mode, see [RequestRestore.Policy].
type RestorePolicy string

// Restore policies the server accepts.
const (
	// RestorePolicyStrict selects the cold restore: the namespace is gated
	// and emptied on every node, then its partitions are hydrated from strict
	// backups only.
	RestorePolicyStrict RestorePolicy = "strict"
	// RestorePolicyFuzzy writes the records into the live namespace, from a
	// single backup of any consistency or from a list of strict backups.
	RestorePolicyFuzzy RestorePolicy = "fuzzy"
)

// StorageType is the object storage type of a backup, see [RequestCommon.Storage].
type StorageType string

// Object storage types the server accepts.
const (
	// StorageTypeFS is a local or mounted file system of each node. Only a
	// strict backup and a cold restore accept it.
	StorageTypeFS StorageType = "fs"
	// StorageTypeFA is the same storage as StorageTypeFS.
	StorageTypeFA StorageType = "fa"
	// StorageTypeAWS is Amazon S3 or an S3-compatible storage.
	StorageTypeAWS StorageType = "aws"
	// StorageTypeAWSS3 is the same storage as StorageTypeAWS.
	StorageTypeAWSS3 StorageType = "aws-s3"
)

// RequestBackup represents a request to start a backup job on the server.
type RequestBackup struct {
	RequestCommon
	// BinList is a comma-separated list of bins to back up.
	BinList string
	// ModifiedBefore is the upper bound of the record last update time, in Unix
	// seconds. It is a string because it is passed to the server verbatim: the
	// server validates the value (decimal seconds after 2010-01-01, no
	// milliseconds) and reports a precise error for a malformed one.
	ModifiedBefore string
	// ModifiedAfter is the lower bound of the record last update time, in Unix
	// seconds. It is a string for the same reason as ModifiedBefore.
	ModifiedAfter string
	// BackupRPS limits the records per second of each scan on each node.
	// The server default is 0, no limit; a non-zero value must be at least
	// 5 × BackupWorkerThreads.
	BackupRPS int
	// BackupWorkerThreads is the number of scan threads per node, from 1 to 128.
	BackupWorkerThreads int
	// BackupUploadThreads is the number of upload threads per node, from 1 to 128.
	BackupUploadThreads int
	// BackupS3ThroughputTargetGbps is the target throughput of the S3 client per
	// node, in Gbit/s, from 1 to 400.
	BackupS3ThroughputTargetGbps int
	// Policy is how consistent the backup must be: strict, best-effort or
	// fuzzy, see [BackupPolicy]. Empty omits the parameter, so the server
	// default, strict, applies.
	Policy BackupPolicy
	// NoConfig skips the upload of the config file of each node, as it is on
	// disk when the backup finishes. The server uploads it by default.
	NoConfig *bool
}

// RequestRestore represents a request to start a restore job on the server.
//
// Policy selects the restore mode, strict (cold) by default. Only the fuzzy
// policy accepts MaxInflight, RetryBaseIntervalMs, RetryMultiplier,
// RetryMaxAttempts, IgnoreRecordError and ApplyMetadataLast, and only the
// strict one accepts HydrateReplica: setting one with another policy fails
// the request, since the server rejects it instead of ignoring it. Every other
// parameter applies to both policies. IncrementalPath needs more than one id
// in BackupIDs.
//
// A cold restore is started by this single request: the server gates and
// prepares the namespace on every node and starts the restore once every node
// is ready. A node whose storage is not empty waits until it is cleared, see
// [ResponseRestoreState.Clear].
type RequestRestore struct {
	RequestCommon
	// JobID identifies the restore job. The server refuses a job id that was
	// used before.
	JobID string
	// BackupIDs is a comma-separated list of backup ids to restore from: a full
	// backup first, then its incremental backups in chronological order.
	BackupIDs string
	// IncrementalPath is the path of the incremental backups, the entries after
	// the first one of BackupIDs, when it differs from Path. Setting it for a
	// single backup id fails the request.
	IncrementalPath string
	// RestoreWorkerThreads is the number of apply threads per node, from 1 to 128.
	RestoreWorkerThreads int
	// RestoreDownloadThreads is the number of download threads per node, from 1 to 128.
	RestoreDownloadThreads int
	// RestoreRPS limits the records per second each node applies. The server
	// default is 0, no limit.
	RestoreRPS int
	// ExtraTTL is added to the expiry of every restored record that expires, in
	// seconds, from 0 to 315360000.
	ExtraTTL int
	// Policy selects the restore mode. strict: cold hydration from strict
	// backups only; the server refuses a fuzzy backup. fuzzy: restore through
	// writes onto the live namespace, from a single backup of any consistency
	// or from a list of strict backups. Empty omits the parameter, so the
	// server default, strict, applies.
	Policy RestorePolicy
	// MaxInflight is the maximum number of outstanding writes per node, from 1
	// to 100000.
	MaxInflight int
	// RetryBaseIntervalMs is the base delay before a retry, in milliseconds.
	// It is a pointer because zero is a valid delay that differs from the
	// server default of 1000; nil omits the parameter.
	RetryBaseIntervalMs *int
	// RetryMultiplier is the backoff multiplier of the retry delay, at least 1.0.
	RetryMultiplier float64
	// RetryMaxAttempts is the number of retries per record after its first
	// failed write, 0 for none. It is a pointer because zero differs from the
	// server default of 5; nil omits the parameter.
	RetryMaxAttempts *int
	// IgnoreRecordError continues the restore on errors of individual records.
	IgnoreRecordError *bool
	// ApplyMetadataLast creates the secondary indexes and UDFs after the records
	// instead of before.
	ApplyMetadataLast *bool
	// HydrateReplica restores every replica of each partition when true, the
	// server default, and only the master when false. Which one a namespace
	// allows depends on its replication factor and consistency mode.
	HydrateReplica *bool
}

// RequestCommon represents common fields for backup and restore requests.
//
// Optional fields of all requests are sent to the server only when they are
// set: strings when not empty, numbers when not zero and pointers when not nil.
// Otherwise the server applies its own default.
type RequestCommon struct {
	// Namespace is the namespace to back up or to restore into.
	Namespace string
	// Storage is the object storage type, see [StorageType].
	Storage StorageType
	// Path is the directory for a file system storage, or the key prefix for S3.
	Path string
	// Bucket is the S3 bucket name.
	Bucket string
	// Region is the AWS region of the bucket.
	Region string
	// Profile is the named AWS profile.
	Profile string
	// Endpoint is the URL of an S3-compatible storage.
	Endpoint string
	// SetList is a comma-separated list of sets.
	SetList string
	// FilterExp is a base64-encoded filter expression.
	FilterExp string
	// NoIndexes skips the secondary indexes.
	NoIndexes *bool
	// NoUDFs skips the UDFs.
	NoUDFs *bool
}
