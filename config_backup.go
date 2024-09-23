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

package backup

import (
	"encoding/base64"
	"fmt"
	"time"

	a "github.com/aerospike/aerospike-client-go/v7"
)

// BackupConfig contains configuration for the backup operation.
type BackupConfig struct {
	// InfoPolicy applies to Aerospike Info requests made during backup and
	// restore. If nil, the Aerospike client's default policy will be used.
	InfoPolicy *a.InfoPolicy
	// ScanPolicy applies to Aerospike scan operations made during backup and
	// restore. If nil, the Aerospike client's default policy will be used.
	ScanPolicy *a.ScanPolicy
	// Only include records that last changed before the given time (optional).
	ModBefore *time.Time
	// Only include records that last changed after the given time (optional).
	ModAfter *time.Time
	// Encryption details.
	EncryptionPolicy *EncryptionPolicy
	// Compression details.
	CompressionPolicy *CompressionPolicy
	// Secret agent config.
	SecretAgentConfig *SecretAgentConfig
	// Namespace is the Aerospike namespace to back up.
	Namespace string
	// NodeList contains a list of nodes to back up.
	// <IP addr 1>:<port 1>[,<IP addr 2>:<port 2>[,...]]
	// <IP addr 1>:<TLS_NAME 1>:<port 1>[,<IP addr 2>:<TLS_NAME 2>:<port 2>[,...]]
	// Backup the given cluster nodes only.
	// If it is set, ParallelNodes automatically set to true.
	// This argument is mutually exclusive to partition-list/AfterDigest arguments.
	NodeList []string
	// SetList is the Aerospike set to back up (optional, given an empty list,
	// all sets will be backed up).
	SetList []string
	// The list of backup bin names
	// (optional, given an empty list, all bins will be backed up)
	BinList []string
	// Partitions specifies the Aerospike partitions to back up.
	Partitions PartitionRange
	// ParallelNodes specifies how to perform scan.
	// If set to true, we launch parallel workers for nodes; otherwise workers run in parallel for partitions.
	// Excludes Partitions param.
	ParallelNodes bool
	// EncoderType describes an Encoder type that will be used on backing up.
	// Default `EncoderTypeASB` = 0.
	EncoderType EncoderType
	// ParallelRead is the number of concurrent scans to run against the Aerospike cluster.
	ParallelRead int
	// ParallelWrite is the number of concurrent backup files writing.
	ParallelWrite int
	// Don't back up any records.
	NoRecords bool
	// Don't back up any secondary indexes.
	NoIndexes bool
	// Don't back up any UDFs.
	NoUDFs bool
	// RecordsPerSecond limits backup records per second (rps) rate.
	// Will not apply rps limit if RecordsPerSecond is zero (default).
	RecordsPerSecond int
	// Limits backup bandwidth (bytes per second).
	// Will not apply rps limit if Bandwidth is zero (default).
	Bandwidth int
	// File size limit (in bytes) for the backup. If a backup file exceeds this
	// size threshold, a new file will be created. 0 for no file size limit.
	FileLimit int64
	// Backup records after record digest in record's partition plus all succeeding partitions.
	// Used to resume backup with last record received from previous incomplete backup.
	// This parameter will overwrite Partitions.Begin value.
	// Can't be used in full backup mode.
	// This parameter is mutually exclusive to partition-list (not implemented).
	// Format: base64 encoded string.
	// Example: EjRWeJq83vEjRRI0VniavN7xI0U=
	AfterDigest string
	// Do not apply base-64 encoding to BLOBs: Bytes, HLL, RawMap, RawList.
	// Results in smaller backup files.
	Compact bool
}

// PartitionRange specifies a range of Aerospike partitions.
type PartitionRange struct {
	Begin int
	Count int
}

// NewPartitionRange returns a partition range with boundaries specified by the
// provided values.
func NewPartitionRange(begin, count int) PartitionRange {
	return PartitionRange{begin, count}
}

func (p PartitionRange) validate() error {
	if p.Begin < 0 || p.Begin >= MaxPartitions {
		return fmt.Errorf("begin must be between 0 and %d, got %d", MaxPartitions-1, p.Begin)
	}

	if p.Count < 1 || p.Count > MaxPartitions {
		return fmt.Errorf("count must be between 1 and %d, got %d", MaxPartitions, p.Count)
	}

	if p.Begin+p.Count > MaxPartitions {
		return fmt.Errorf("begin + count is greater than the max partitions count of %d",
			MaxPartitions)
	}

	return nil
}

// PartitionRangeAll returns a partition range containing all partitions.
func PartitionRangeAll() PartitionRange {
	return NewPartitionRange(0, MaxPartitions)
}

// NewDefaultBackupConfig returns a new BackupConfig with default values.
func NewDefaultBackupConfig() *BackupConfig {
	return &BackupConfig{
		Partitions:    PartitionRangeAll(),
		ParallelRead:  1,
		ParallelWrite: 1,
		Namespace:     "test",
		EncoderType:   EncoderTypeASB,
	}
}

func (c *BackupConfig) isFullBackup() bool {
	// full backup doesn't have lower bound
	return c.ModAfter == nil && c.AfterDigest == ""
}

//nolint:gocyclo // Long validation function with a lot of checks.
func (c *BackupConfig) validate() error {
	if c.ParallelRead < MinParallel || c.ParallelRead > MaxParallel {
		return fmt.Errorf("parallel read must be between 1 and 1024, got %d", c.ParallelRead)
	}

	if c.ParallelWrite < MinParallel || c.ParallelWrite > MaxParallel {
		return fmt.Errorf("parallel write must be between 1 and 1024, got %d", c.ParallelWrite)
	}

	if c.ModBefore != nil && c.ModAfter != nil && !c.ModBefore.After(*c.ModAfter) {
		return fmt.Errorf("modified before must be strictly greater than modified after")
	}

	if (c.ParallelNodes || len(c.NodeList) != 0) && (c.Partitions.Begin != 0 || c.Partitions.Count != 0) {
		return fmt.Errorf("parallel by nodes and partitions and the same time not allowed")
	}

	if !c.ParallelNodes && len(c.NodeList) == 0 {
		if err := c.Partitions.validate(); err != nil {
			return err
		}
	}

	if c.AfterDigest != "" {
		if c.ParallelNodes || len(c.NodeList) != 0 {
			return fmt.Errorf("parallel by nodes/node list and after digest at the same time not allowed")
		}

		if _, err := base64.StdEncoding.DecodeString(c.AfterDigest); err != nil {
			return fmt.Errorf("after digest must be base64 encoded string: %w", err)
		}

		if c.Partitions.Begin != 0 {
			return fmt.Errorf("after digest is set, begin partiotion can't be set")
		}
	}

	if c.RecordsPerSecond < 0 {
		return fmt.Errorf("rps value must not be negative, got %d", c.RecordsPerSecond)
	}

	if c.Bandwidth < 0 {
		return fmt.Errorf("bandwidth value must not be negative, got %d", c.Bandwidth)
	}

	if c.FileLimit < 0 {
		return fmt.Errorf("filelimit value must not be negative, got %d", c.FileLimit)
	}

	if err := c.CompressionPolicy.validate(); err != nil {
		return fmt.Errorf("compression policy invalid: %w", err)
	}

	if err := c.EncryptionPolicy.validate(); err != nil {
		return fmt.Errorf("encryption policy invalid: %w", err)
	}

	if err := c.SecretAgentConfig.validate(); err != nil {
		return fmt.Errorf("secret agent invalid: %w", err)
	}

	return nil
}
