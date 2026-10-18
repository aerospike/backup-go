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
}

// RequestRestore represents a request to start a restore job on the server.
//
// Some parameters apply to one restore mode only. Setting a parameter that does
// not apply to the mode chosen by FuzzyRestore fails the request, since the
// server rejects such a parameter instead of ignoring it.
type RequestRestore struct {
	RequestCommon
	// JobID identifies the restore job. A cold restore must use the job id that
	// was passed to the restore preparation.
	JobID string
	// BackupIDs is a comma-separated list of backup ids to restore from: a full
	// backup first, then its incremental backups in chronological order.
	BackupIDs string
	// IncrementalPath is the path of the incremental backups, the entries after
	// the first one of BackupIDs, when it differs from Path.
	IncrementalPath string
	// FuzzyRestore restores by writing records into the live namespace instead
	// of the cold partition hydration. FilterExp applies to the cold restore
	// only; the fields marked as fuzzy only apply when FuzzyRestore is true.
	FuzzyRestore *bool
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
	// AllowUnhosted allows the restore even if the principal does not host the
	// namespace. Fuzzy only.
	AllowUnhosted *bool
	// MaxInflight is the maximum number of outstanding writes per node, from 1
	// to 100000. Fuzzy only.
	MaxInflight int
	// RetryBaseIntervalMs is the base delay before a retry, in milliseconds.
	// It is a pointer because zero is a valid delay that differs from the
	// server default of 1000; nil omits the parameter. Fuzzy only.
	RetryBaseIntervalMs *int
	// RetryMultiplier is the backoff multiplier of the retry delay, at least 1.0.
	// Fuzzy only.
	RetryMultiplier float64
	// RetryMaxAttempts is the maximum number of attempts per record, the first
	// one included. Zero omits the parameter, so the server default of 5
	// applies; use 1 for no retry. Fuzzy only.
	RetryMaxAttempts int
	// IgnoreRecordError continues the restore on errors of individual records.
	// Fuzzy only.
	IgnoreRecordError *bool
	// ApplyMetadataLast creates the secondary indexes and UDFs after the records
	// instead of before. Fuzzy only.
	ApplyMetadataLast *bool
}

// RequestPrepareRestore represents a request to prepare a cold restore on the server.
type RequestPrepareRestore struct {
	// Namespace is the target namespace.
	Namespace string
	// JobID identifies the restore job; the restore must be started with the same id.
	JobID string
	// HydrateReplica restores all replicas when true, and only the master when false.
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
	// Storage is the object storage type, such as "fs" or "aws-s3".
	Storage string
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
	// FilterExp is a base64-encoded filter expression. A restore accepts it
	// only in the cold mode.
	FilterExp string
	// NoIndexes skips the secondary indexes.
	NoIndexes *bool
	// NoUDFs skips the UDFs.
	NoUDFs *bool
}
