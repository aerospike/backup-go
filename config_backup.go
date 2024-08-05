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
	// SetList is the Aerospike set to back up (optional, given an empty list,
	// all sets will be backed up).
	SetList []string
	// The list of backup bin names
	// (optional, given an empty list, all bins will be backed up)
	BinList []string
	// Partitions specifies the Aerospike partitions to back up.
	Partitions PartitionRange
	// EncoderType describes an Encoder type that will be used on backing up.
	// Default `EncoderTypeASB` = 0.
	EncoderType EncoderType
	// Parallel is the number of concurrent scans to run against the Aerospike
	// cluster.
	Parallel int
	// Don't back up any records.
	NoRecords bool
	// Don't back up any secondary indexes.
	NoIndexes bool
	// Don't back up any UDFs.
	NoUDFs bool
	// Exclude bins data from backup.
	NoBins bool
	// RecordsPerSecond limits backup records per second (rps) rate.
	// Will not apply rps limit if RecordsPerSecond is zero (default).
	RecordsPerSecond int
	// Limits backup bandwidth (bytes per second).
	// Will not apply rps limit if Bandwidth is zero (default).
	Bandwidth int
	// File size limit (in bytes) for the backup. If a backup file exceeds this
	// size threshold, a new file will be created. 0 for no file size limit.
	FileLimit int64
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
		Partitions:  PartitionRange{0, MaxPartitions},
		Parallel:    1,
		Namespace:   "test",
		EncoderType: EncoderTypeASB,
	}
}

func (c *BackupConfig) isFullBackup() bool {
	// full backup doesn't have lower bound
	return c.ModAfter == nil
}

func (c *BackupConfig) validate() error {
	if c.Parallel < MinParallel || c.Parallel > MaxParallel {
		return fmt.Errorf("parallel must be between 1 and 1024, got %d", c.Parallel)
	}

	if c.ModBefore != nil && c.ModAfter != nil && !c.ModBefore.After(*c.ModAfter) {
		return fmt.Errorf("modified before must be strictly greater than modified after")
	}

	if err := c.Partitions.validate(); err != nil {
		return err
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

	if err := c.CompressionPolicy.Validate(); err != nil {
		return fmt.Errorf("compression policy invalid: %w", err)
	}

	if err := c.EncryptionPolicy.Validate(); err != nil {
		return fmt.Errorf("encryption policy invalid: %w", err)
	}

	if err := c.SecretAgentConfig.Validate(); err != nil {
		return fmt.Errorf("secret agent invalid: %w", err)
	}

	return nil
}
