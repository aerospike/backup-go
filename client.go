// Copyright 2024-2024 Aerospike, Inc.
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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/encoding"
	"github.com/aerospike/backup-go/encoding/asb"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aerospike/backup-go/models"
)

const (
	// MinParallel is the minimum number of workers to use during an operation.
	MinParallel = 1
	// MaxParallel is the maximum number of workers to use during an operation.
	MaxParallel = 1024
	// MaxPartitions is the maximum number of partitions in an Aerospike cluster.
	MaxPartitions = 4096
)

var (
	defaultEncoderFactory = asb.NewASBEncoderFactory()
	defaultDecoderFactory = asb.NewASBDecoderFactory()
)

// Client is the main entry point for the backup package.
// It wraps an aerospike client and provides methods to start backup and restore operations.
// Example usage:
//
//		asc, aerr := a.NewClientWithPolicy(...)	// create an aerospike client
//		if aerr != nil {
//			// handle error
//		}
//		backupClient, err := backup.NewClient(asc, "id", nil)	// create a backup client
//		if err != nil {
//			// handle error
//		}
//		// use the backup client to start backup and restore operations
//	 ctx := context.Background()
//		backupHandler, err := backupClient.Backup(ctx, writers, nil)
//		if err != nil {
//			// handle error
//	 }
//	 // optionally, check the stats of the backup operation
//		stats := backupHandler.Stats()
//		// use the backupHandler to wait for the backup operation to finish
//	 ctx := context.Background()
//		// err = backupHandler.Wait(ctx)
type Client struct {
	aerospikeClient *a.Client
	logger          *slog.Logger
	id              string
}

// NewClient creates a new backup client.
// ac is the aerospike client to use for backup and restore operations.
// id is an identifier for the client.
// logger is the logger that this client will log to.
func NewClient(ac *a.Client, id string, logger *slog.Logger) (*Client, error) {
	if ac == nil {
		return nil, errors.New("aerospike client pointer is nil")
	}

	if logger == nil {
		logger = slog.Default()
	}

	// qualify the logger with a backup lib group
	logger = logger.WithGroup("backup")

	// add a client group to the logger
	logger = logging.WithClient(logger, id)

	return &Client{
		aerospikeClient: ac,
		id:              id,
		logger:          logger,
	}, nil
}

func (c *Client) getUsableInfoPolicy(p *a.InfoPolicy) a.InfoPolicy {
	if p == nil {
		p = c.aerospikeClient.DefaultInfoPolicy
	}

	return *p
}

func (c *Client) getUsableWritePolicy(p *a.WritePolicy) a.WritePolicy {
	if p == nil {
		p = c.aerospikeClient.DefaultWritePolicy
	}

	return *p
}

func (c *Client) getUsableScanPolicy(p *a.ScanPolicy) a.ScanPolicy {
	if p == nil {
		p = c.aerospikeClient.DefaultScanPolicy
	}

	return *p
}

// **** Backup ****

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

// PartitionRangeAll returns a partition range containing all partitions.
func PartitionRangeAll() PartitionRange {
	return NewPartitionRange(0, MaxPartitions)
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

// BackupConfig contains configuration for the backup operation.
type BackupConfig struct {
	// EncoderFactory is used to specify the encoder with which to encode the backup data
	// if nil, the default EncoderFactory will be used.
	EncoderFactory encoding.EncoderFactory
	// InfoPolicy applies to Aerospike Info requests made during backup and restore
	// If nil, the Aerospike client's default policy will be used.
	InfoPolicy *a.InfoPolicy
	// ScanPolicy applies to Aerospike scan operations made during backup and restore
	// If nil, the Aerospike client's default policy will be used.
	ScanPolicy *a.ScanPolicy
	// Only include records that last changed before the given time (optional).
	ModBefore *time.Time
	// Only include records that last changed after the given time (optional).
	ModAfter *time.Time
	// Encryption details.
	EncryptionPolicy *models.EncryptionPolicy
	// Compression details.
	CompressionPolicy *models.CompressionPolicy
	// Namespace is the Aerospike namespace to back up.
	Namespace string
	// SetList is the Aerospike set to back up (optional, given an empty list,
	// all sets will be backed up).
	SetList []string
	// The list of backup bin names (optional, given an empty list, all bins will be backed up)
	BinList []string
	// Partitions specifies the Aerospike partitions to back up.
	Partitions PartitionRange
	// Parallel is the number of concurrent scans to run against the Aerospike cluster.
	Parallel int
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
	// File size limit (in bytes) for the backup. If a backup file crosses this size threshold,
	// a new file will be created. 0 for no file size limit.
	FileLimit int64
}

func (c *BackupConfig) validate() error {
	if c.Parallel < MinParallel || c.Parallel > MaxParallel {
		return fmt.Errorf("parallel must be between 1 and 1024, got %d", c.Parallel)
	}

	if c.ModBefore != nil && c.ModAfter != nil && !c.ModBefore.After(*c.ModAfter) {
		return errors.New("modified before must be strictly greater than modified after")
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

	return nil
}

// NewBackupConfig returns a new BackupConfig with default values.
func NewBackupConfig() *BackupConfig {
	return &BackupConfig{
		Partitions:     PartitionRange{0, MaxPartitions},
		Parallel:       1,
		Namespace:      "test",
		EncoderFactory: defaultEncoderFactory,
	}
}

func (c *BackupConfig) isFullBackup() bool {
	// full backup doesn't have lower bound
	return c.ModAfter == nil
}

// Backup starts a backup operation that writes data to a provided writer.
// ctx can be used to cancel the backup operation.
// config is the configuration for the backup operation.
// writer creates new writers for the backup operation.
func (c *Client) Backup(ctx context.Context, config *BackupConfig, writer WriteFactory) (
	*BackupHandler, error) {
	if config == nil {
		return nil, fmt.Errorf("backup config required")
	}

	// copy the policies so we don't modify the original
	infoPolicy := c.getUsableInfoPolicy(config.InfoPolicy)
	config.InfoPolicy = &infoPolicy

	scanPolicy := c.getUsableScanPolicy(config.ScanPolicy)
	config.ScanPolicy = &scanPolicy

	if err := config.validate(); err != nil {
		return nil, err
	}

	handler := newBackupHandler(config, c.aerospikeClient, c.logger, writer)
	handler.run(ctx)

	return handler, nil
}

// **** Restore ****

// RestoreConfig contains configuration for the restore operation.
type RestoreConfig struct {
	// DecoderFactory is used to specify the decoder with which to decode the backup data
	// if nil, the default DecoderFactory will be used.
	DecoderFactory encoding.DecoderFactory
	// InfoPolicy applies to Aerospike Info requests made during backup and restore
	// If nil, the Aerospike client's default policy will be used.
	InfoPolicy *a.InfoPolicy
	// WritePolicy applies to Aerospike write operations made during backup and restore
	// If nil, the Aerospike client's default policy will be used.
	WritePolicy *a.WritePolicy
	// Namespace details for the restore operation.
	// By default, the data is restored to the namespace from which it was taken.
	Namespace *models.RestoreNamespace `json:"namespace,omitempty"`
	// Encryption details.
	EncryptionPolicy *models.EncryptionPolicy
	// Compression details.
	CompressionPolicy *models.CompressionPolicy
	// The sets to restore (optional, given an empty list, all sets will be restored).
	SetList []string
	// The bins to restore (optional, given an empty list, all bins will be restored).
	BinList []string
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
	NoIndexes bool
	// Don't restore any UDFs.
	NoUDFs bool
	// Disables the use of batch writes when restoring records to the Aerospike cluster.
	DisableBatchWrites bool
	// The max allowed number of records per batch write call.
	BatchSize int
	// Max number of parallel writers to target AS cluster.
	MaxAsyncBatches int
}

func (c *RestoreConfig) validate() error {
	if c.Parallel < MinParallel || c.Parallel > MaxParallel {
		return fmt.Errorf("parallel must be between 1 and 1024, got %d", c.Parallel)
	}

	if c.Namespace != nil {
		if err := c.Namespace.Validate(); err != nil {
			return fmt.Errorf("invalid restore namespace: %w", err)
		}
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

	if err := c.CompressionPolicy.Validate(); err != nil {
		return fmt.Errorf("compression policy invalid: %w", err)
	}

	if err := c.EncryptionPolicy.Validate(); err != nil {
		return fmt.Errorf("encryption policy invalid: %w", err)
	}

	return nil
}

// NewRestoreConfig returns a new RestoreConfig with default values.
func NewRestoreConfig() *RestoreConfig {
	return &RestoreConfig{
		Parallel:        4,
		DecoderFactory:  defaultDecoderFactory,
		BatchSize:       128,
		MaxAsyncBatches: 16,
	}
}

// Restore starts a restore operation that reads data from given readers.
// The backup data may be in a single file or multiple files.
// ctx can be used to cancel the restore operation.
// config is the configuration for the restore operation.
// streamingReader provides readers with access to backup data.
func (c *Client) Restore(ctx context.Context, config *RestoreConfig, streamingReader StreamingReader,
) (*RestoreHandler, error) {
	if config == nil {
		return nil, fmt.Errorf("restore config required")
	}

	// copy the policies so we don't modify the original
	infoPolicy := c.getUsableInfoPolicy(config.InfoPolicy)
	config.InfoPolicy = &infoPolicy

	writePolicy := c.getUsableWritePolicy(config.WritePolicy)
	config.WritePolicy = &writePolicy

	if err := config.validate(); err != nil {
		return nil, err
	}

	handler := newRestoreHandler(config, c.aerospikeClient, c.logger, streamingReader)
	handler.startAsync(ctx)

	return handler, nil
}
