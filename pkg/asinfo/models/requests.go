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
	// ModifiedBefore is the upper bound of the record last update time, in Unix seconds.
	ModifiedBefore string
	// ModifiedAfter is the lower bound of the record last update time, in Unix seconds.
	ModifiedAfter string
}

// RequestRestore represents a request to start a restore job on the server.
type RequestRestore struct {
	RequestCommon
	// JobID identifies the restore job. A cold restore must use the job id that
	// was passed to the restore preparation.
	JobID string
	// BackupID identifies the backup to restore from.
	BackupID string
	// FuzzyRestore restores by writing records into the live namespace instead
	// of the cold partition hydration. The fields below are sent only when it is
	// true.
	FuzzyRestore *bool
	// AllowUnhosted allows the restore even if the principal does not host the namespace.
	AllowUnhosted *bool
	// Parallel is the number of worker threads.
	Parallel int
	// RecordsPerSecond limits the write rate.
	RecordsPerSecond int
	// MaxInflight is the maximum number of concurrent writes.
	MaxInflight int
	// RetryBaseIntervalMs is the base delay before a retry, in milliseconds.
	RetryBaseIntervalMs int
	// RetryMultiplier is the backoff multiplier of the retry delay.
	RetryMultiplier float64
	// RetryMaxAttempts is the maximum number of attempts per record.
	RetryMaxAttempts int
	// IgnoreRecordError continues the restore on errors of individual records.
	IgnoreRecordError *bool
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
// set: strings when not empty, numbers when not zero and bool pointers when not
// nil. Otherwise the server applies its own default.
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
	// FilterExp is a base64-encoded filter expression.
	FilterExp string
	// NoIndexes skips the secondary indexes.
	NoIndexes *bool
	// NoUDFs skips the UDFs.
	NoUDFs *bool
}
