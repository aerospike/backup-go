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

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/internal/logging"
)

const (
	// MinParallel is the minimum number of workers to use during an operation.
	MinParallel = 1
	// MaxParallel is the maximum number of workers to use during an operation.
	MaxParallel = 1024
	// MaxPartitions is the maximum number of partitions in an Aerospike cluster.
	MaxPartitions = 4096
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

	if err := c.SecretAgentConfig.Validate(); err != nil {
		return fmt.Errorf("secret agent invalid: %w", err)
	}

	return nil
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

	if err := c.SecretAgent.Validate(); err != nil {
		return fmt.Errorf("secret agent invalid: %w", err)
	}

	return nil
}

// Restore starts a restore operation that reads data from given readers.
// The backup data may be in a single file or multiple files.
// ctx can be used to cancel the restore operation.
// config is the configuration for the restore operation.
// streamingReader provides readers with access to backup data.
func (c *Client) Restore(
	ctx context.Context,
	config *RestoreConfig,
	streamingReader StreamingReader,
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
