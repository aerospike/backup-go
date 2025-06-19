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

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
)

// ConfigRestore contains configuration for the restore operation.
type ConfigRestore struct {
	// InfoPolicy applies to Aerospike Info requests made during backup and restore
	// If nil, the Aerospike client's default policy will be used.
	InfoPolicy *a.InfoPolicy
	// WritePolicy applies to Aerospike write operations made during backup and restore
	// If nil, the Aerospike client's default policy will be used.
	WritePolicy *a.WritePolicy
	// Namespace details for the restore operation.
	// By default, the data is restored to the namespace from which it was taken.
	Namespace *RestoreNamespaceConfig `json:"namespace,omitempty"`
	// Encryption details.
	EncryptionPolicy *EncryptionPolicy
	// Compression details.
	CompressionPolicy *CompressionPolicy
	// Configuration of retries for each restore write operation.
	// If nil, no retries will be performed.
	RetryPolicy *models.RetryPolicy
	// Secret agent config.
	SecretAgentConfig *SecretAgentConfig
	// The sets to restore (optional, given an empty list, all sets will be restored).
	// Not applicable for XDR restore.
	SetList []string
	// The bins to restore (optional, given an empty list, all bins will be restored).
	// Not applicable for XDR restore.
	BinList []string
	// EncoderType describes an Encoder type that will be used on restoring.
	// Default `EncoderTypeASB` = 0.
	EncoderType EncoderType
	// Parallel is the number of concurrent record readers from backup files.
	Parallel int
	// RecordsPerSecond limits restore records per second (rps) rate.
	// Will not apply rps limit if RecordsPerSecond is zero (default).
	RecordsPerSecond int
	// Limits restore bandwidth (bytes per second).
	// Will not apply rps limit if Bandwidth is zero (default).
	Bandwidth int
	// Don't restore any records.
	NoRecords bool
	// Don't restore any secondary indexes.
	// Not applicable for XDR restore.
	NoIndexes bool
	// Don't restore any UDFs.
	// Not applicable for XDR restore.
	NoUDFs bool
	// Disables the use of batch writes when restoring records to the Aerospike cluster.
	// Not applicable for XDR restore.
	DisableBatchWrites bool
	// The max allowed number of records per batch write call.
	// Not applicable for XDR restore.
	BatchSize int
	// Max number of parallel writers to target AS cluster.
	// Not applicable for XDR restore.
	MaxAsyncBatches int
	// Amount of extra time-to-live to add to records that have expirable void-times.
	// Must be set in seconds.
	// Not applicable for XDR restore.
	ExtraTTL int64
	// Ignore permanent record-specific error.
	// E.g.: AEROSPIKE_RECORD_TOO_BIG.
	// By default, such errors are not ignored and restore terminates.
	// Not applicable for XDR restore.
	IgnoreRecordError bool
	// Retry policy for info commands.
	InfoRetryPolicy *models.RetryPolicy
	// MetricsEnabled indicates whether backup metrics collection and reporting are enabled.
	MetricsEnabled bool
	// ValidateOnly indicates whether restore should only validate the backup files.
	ValidateOnly bool
}

// NewDefaultRestoreConfig returns a new ConfigRestore with default values.
func NewDefaultRestoreConfig() *ConfigRestore {
	return &ConfigRestore{
		Parallel:        4,
		BatchSize:       128,
		MaxAsyncBatches: 16,
		EncoderType:     EncoderTypeASB,
	}
}

func (c *ConfigRestore) validate() error {
	if c.Parallel < MinParallel || c.Parallel > MaxParallel {
		return fmt.Errorf("parallel must be between 1 and 1024, got %d", c.Parallel)
	}

	if err := c.Namespace.validate(); err != nil {
		return fmt.Errorf("invalid restore namespace: %w", err)
	}

	if c.Bandwidth < 0 {
		return fmt.Errorf("bandwidth value must not be negative, got %d", c.Bandwidth)
	}

	if c.RecordsPerSecond < 0 {
		return fmt.Errorf("rps value must not be negative, got %d", c.RecordsPerSecond)
	}

	if c.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive, got %d", c.BatchSize)
	}

	if c.MaxAsyncBatches <= 0 {
		return fmt.Errorf("max async batches must be positive, got %d", c.MaxAsyncBatches)
	}

	if c.ExtraTTL < 0 {
		return fmt.Errorf("extra ttl value must not be negative, got %d", c.ExtraTTL)
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

// isValidForASBX checks if config is valid for restoring from asbx.
func (c *ConfigRestore) isValidForASBX() error {
	if c.Namespace != nil && *c.Namespace.Source != *c.Namespace.Destination {
		return fmt.Errorf("changing namespace is not supported for ASBX")
	}

	if len(c.SetList) > 0 {
		return fmt.Errorf("set list is not supported for ASBX")
	}

	if len(c.BinList) > 0 {
		return fmt.Errorf("bin list is not supported for ASBX")
	}

	if c.NoRecords {
		return fmt.Errorf("no records is not supported for ASBX")
	}

	if c.NoIndexes {
		return fmt.Errorf("no indexes is not supported for ASBX")
	}

	if c.NoUDFs {
		return fmt.Errorf("no udfs is not supported for ASBX")
	}

	if c.DisableBatchWrites {
		return fmt.Errorf("disable batch writes is not supported for ASBX")
	}

	if c.ExtraTTL > 0 {
		return fmt.Errorf("extra ttl value is not supported for ASBX")
	}

	return nil
}
