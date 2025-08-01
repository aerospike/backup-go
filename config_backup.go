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

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
)

// ConfigBackup contains configuration for the backup operation.
type ConfigBackup struct {
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
	// PartitionFilters specifies the Aerospike partitions to back up.
	// Partition filters can be ranges, individual partitions,
	// or records after a specific digest within a single partition.
	// Note:
	// if not default partition filter NewPartitionFilterAll() is used,
	// each partition filter is an individual task which cannot be parallelized,
	// so you can only achieve as much parallelism as there are partition filters.
	// You may increase parallelism by dividing up partition ranges manually.
	// AfterDigest:
	// afterDigest filter can be applied with
	// NewPartitionFilterAfterDigest(namespace, digest string) (*a.PartitionFilter, error)
	// Backup records after record digest in record's partition plus all succeeding partitions.
	// Used to resume backup with last record received from previous incomplete backup.
	// This parameter will overwrite PartitionFilters.Begin value.
	// Can't be used in full backup mode.
	// This parameter is mutually exclusive with partition-list (not implemented).
	// Format: base64 encoded string.
	// Example: EjRWeJq83vEjRRI0VniavN7xI0U=
	PartitionFilters []*a.PartitionFilter
	// Namespace is the Aerospike namespace to back up.
	Namespace string
	// NodeList contains a list of nodes to back up.
	// <addr 1>:<port 1>[,<addr 2>:<port 2>[,...]] or <node name 1>[,<node name 2>[,...]]
	// To get the correct node address, use 'service-tls-std' if a database configured to use TLS
	// and 'service-clear-std' info command if no TLS is configured.
	// To get the node name, use the 'node:' info command.
	// Backup the given cluster nodes only.
	// If it is set, ParallelNodes automatically set to true.
	// This argument is mutually exclusive with partition-list/AfterDigest arguments.
	NodeList []string
	// SetList is the Aerospike set to back up (optional, given an empty list,
	// all sets will be backed up).
	SetList []string
	// The list of backup bin names
	// (optional, given an empty list, all bins will be backed up)
	BinList []string
	// The list of rack ids.
	// (optional, given an empty list, all racks will be backed up)
	RackList []int
	// ParallelNodes specifies how to perform scan.
	// If set to true, we launch parallel workers for nodes; otherwise workers run in parallel for partitions.
	// Excludes PartitionFilters param.
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
	// The lower bound is 8MiB (maximum size of the Aerospike record).
	// Effective limit value is calculated using the formula:
	// Bandwidth * base64ratio + metaOverhead
	// Where: base64ratio = 1.34, metaOverhead = 16 * 1024
	// Will not apply rps limit if Bandwidth is zero (default).
	Bandwidth int64
	// File size limit (in bytes) for the backup. If a backup file exceeds this
	// size threshold, a new file will be created. 0 for no file size limit.
	FileLimit uint64
	// Do not apply base-64 encoding to BLOBs: Bytes, HLL, RawMap, RawList.
	// Results in smaller backup files.
	Compact bool
	// Only include records that have no ttl set (persistent records).
	NoTTLOnly bool
	// Name of a state file that will be saved in backup directory.
	// Works only with FileLimit parameter.
	// As we reach FileLimit and close file, the current state will be saved.
	// Works only for default and/or partition backup.
	// Not work with ParallelNodes or NodeList.
	StateFile string
	// Resumes an interrupted/failed backup from where it was left off, given the .state file
	// that was generated from the interrupted/failed run.
	// Works only for default and/or partition backup. Not work with ParallelNodes or NodeList.
	Continue bool
	// How many records will be read on one iteration for continuation backup.
	// Affects size if overlap on resuming backup after an error.
	// By default, it must be zero. If any value is set, reading from Aerospike will be paginated.
	// Which affects the performance and RAM usage.
	PageSize int64
	// When using directory parameter, prepend a prefix to the names of the generated files.
	OutputFilePrefix string
	// Retry policy for info commands.
	InfoRetryPolicy *models.RetryPolicy
	// MetricsEnabled indicates whether backup metrics collection and reporting are enabled.
	MetricsEnabled bool
}

// NewDefaultBackupConfig returns a new ConfigBackup with default values.
func NewDefaultBackupConfig() *ConfigBackup {
	return &ConfigBackup{
		PartitionFilters: []*a.PartitionFilter{NewPartitionFilterAll()},
		ParallelRead:     1,
		ParallelWrite:    1,
		Namespace:        "test",
		EncoderType:      EncoderTypeASB,
		ScanPolicy:       a.NewScanPolicy(),
	}
}

// isParalleledByNodes determines whether the backup is parallelized by nodes.
func (c *ConfigBackup) isParalleledByNodes() bool {
	return c.ParallelNodes || len(c.NodeList) > 0 || len(c.RackList) > 0
}

// isDefaultPartitionFilter checks if the default filter is set.
func (c *ConfigBackup) isDefaultPartitionFilter() bool {
	return len(c.PartitionFilters) == 1 &&
		c.PartitionFilters[0].Begin == 0 &&
		c.PartitionFilters[0].Count == MaxPartitions &&
		c.PartitionFilters[0].Digest == nil
}

// isStateFirstRun checks if it is the first run of the backup with a state file.
func (c *ConfigBackup) isStateFirstRun() bool {
	return c.StateFile != "" && !c.Continue
}

// isStateContinueRun checks if we continue backup from a state file.
func (c *ConfigBackup) isStateContinue() bool {
	return c.StateFile != "" && c.Continue
}

func (c *ConfigBackup) withoutFilter() bool {
	return c.ModAfter == nil && c.ScanPolicy.FilterExpression == nil
}

// validate validates the ConfigBackup.
//
//nolint:gocyclo // contains a long list of validations
func (c *ConfigBackup) validate() error {
	if c.ParallelRead < MinParallel || c.ParallelRead > MaxParallel {
		return fmt.Errorf("parallel read must be between 1 and 1024, got %d", c.ParallelRead)
	}

	if c.ParallelWrite < MinParallel || c.ParallelWrite > MaxParallel {
		return fmt.Errorf("parallel write must be between 1 and 1024, got %d", c.ParallelWrite)
	}

	if c.ModBefore != nil && c.ModAfter != nil && !c.ModBefore.After(*c.ModAfter) {
		return fmt.Errorf("modified before must be strictly greater than modified after")
	}

	if c.isParalleledByNodes() && !c.isDefaultPartitionFilter() {
		return fmt.Errorf("parallel by nodes, racks and/or and after digest/partition and the same time not allowed")
	}

	if c.RecordsPerSecond < 0 {
		return fmt.Errorf("rps value must not be negative, got %d", c.RecordsPerSecond)
	}

	if c.Bandwidth < 0 {
		return fmt.Errorf("bandwidth value must not be negative, got %d", c.Bandwidth)
	}

	if c.StateFile != "" && c.PageSize == 0 {
		return fmt.Errorf("page size must be set if saving state to state file is enabled")
	}

	if c.StateFile != "" && c.FileLimit == 0 {
		return fmt.Errorf("file limit must be set if saving state to state file is enabled")
	}

	if c.Continue && c.StateFile == "" {
		return fmt.Errorf("state file must be set if continue is enabled")
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

	for i := range c.SetList {
		if c.SetList[i] == models.MonitorRecordsSetName {
			return fmt.Errorf("mrt monitor set is not allowed for backup")
		}
	}

	if err := c.InfoRetryPolicy.Validate(); err != nil {
		return fmt.Errorf("invalid info retry policy: %w", err)
	}

	if c.EncoderType != EncoderTypeASB {
		return fmt.Errorf("unsuported encoder type: %d", c.EncoderType)
	}

	if c.ScanPolicy.ReplicaPolicy == a.PREFER_RACK && (len(c.RackList) != 0 || len(c.NodeList) != 0) {
		return fmt.Errorf("racks list or nodes list are not supported with PREFER_RACK replica policy")
	}

	return nil
}
