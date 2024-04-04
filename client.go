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
	"io"
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/encoding"
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

var (
	defaultEncoderFactory = encoding.NewASBEncoderFactory()
	defaultDecoderFactory = encoding.NewASBDecoderFactory()
)

// **** Client ****

// Config contains configuration for the backup client.
type Config struct{}

// NewConfig returns a new client Config.
func NewConfig() *Config {
	return &Config{}
}

// Client is the main entry point for the backup package.
// It wraps an aerospike client and provides methods to start backup and restore operations.
// Example usage:
//
//		asc, aerr := a.NewClientWithPolicy(...)	// create an aerospike client
//		if aerr != nil {
//			// handle error
//		}
//		backupCFG := backup.NewConfig()	// create a backup config
//		backupClient, err := backup.NewClient(asc, backupCFG)	// create a backup client
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
	config          *Config
	logger          *slog.Logger
	id              string
}

// NewClient creates a new backup client.
// ac is the aerospike client to use for backup and restore operations.
// id is an identifier for the client.
// logger is the logger that this client will log to.
// config is the configuration for the backup client.
func NewClient(ac *a.Client, id string, logger *slog.Logger, config *Config) (*Client, error) {
	if ac == nil {
		return nil, errors.New("aerospike client pointer is nil")
	}

	if logger == nil {
		logger = slog.Default()
	}

	if config == nil {
		config = NewConfig()
	}

	// qualify the logger with a backup lib group
	logger = logger.WithGroup("backup")

	// add a client group to the logger
	logger = logging.WithClient(logger, id)

	return &Client{
		aerospikeClient: ac,
		id:              id,
		config:          config,
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

// EncoderFactory is used to specify the encoder with which to encode the backup data
// if nil, the default encoder factory will be used.
type EncoderFactory interface {
	CreateEncoder() (encoding.Encoder, error)
}

// PartitionRange specifies a range of Aerospike partitions.
type PartitionRange struct {
	Begin int
	Count int
}

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
		return fmt.Errorf("begin + count is greater than the max partitions count of %d", MaxPartitions)
	}

	return nil
}

// BackupConfig contains configuration for the backup operation.
type BackupConfig struct {
	// EncoderFactory is used to specify the encoder with which to encode the backup data
	// if nil, the default EncoderFactory will be used.
	EncoderFactory EncoderFactory
	// InfoPolicy applies to Aerospike Info requests made during backup and restore
	// If nil, the Aerospike client's default policy will be used.
	InfoPolicy *a.InfoPolicy
	// ScanPolicy applies to Aerospike scan operations made during backup and restore
	// If nil, the Aerospike client's default policy will be used.
	ScanPolicy *a.ScanPolicy
	// Namespace is the Aerospike namespace to backup.
	Namespace string
	// Set is the Aerospike set to backup.
	Set string
	// Partitions specifies the Aerospike partitions to backup.
	Partitions PartitionRange
	// parallel is the number of concurrent scans to run against the Aerospike cluster.
	Parallel int
}

func (c *BackupConfig) validate() error {
	if c.Parallel < MinParallel || c.Parallel > MaxParallel {
		return fmt.Errorf("parallel must be between 1 and 1024, got %d", c.Parallel)
	}

	err := c.Partitions.validate()
	if err != nil {
		return err
	}

	return nil
}

// NewBackupConfig returns a new BackupConfig with default values.
func NewBackupConfig() *BackupConfig {
	return &BackupConfig{
		Partitions:     PartitionRange{0, MaxPartitions},
		Parallel:       1,
		Set:            "",
		Namespace:      "test",
		EncoderFactory: defaultEncoderFactory,
	}
}

// Backup starts a backup operation to a set of io.writers.
// ctx can be used to cancel the backup operation.
// writers is a set of io.writers to write the backup data to.
// config is the configuration for the backup operation.
func (c *Client) Backup(ctx context.Context, writers []io.Writer, config *BackupConfig) (*BackupHandler, error) {
	if config == nil {
		config = NewBackupConfig()
	}

	// copy the policies so we don't modify the original
	infoPolicy := c.getUsableInfoPolicy(config.InfoPolicy)
	config.InfoPolicy = &infoPolicy

	scanPolicy := c.getUsableScanPolicy(config.ScanPolicy)
	config.ScanPolicy = &scanPolicy

	if err := config.validate(); err != nil {
		return nil, err
	}

	handler := newBackupHandler(config, c.aerospikeClient, writers, c.logger)
	handler.run(ctx)

	return handler, nil
}

// BackupToDirectoryConfig contains configuration for the backup to directory operation.
type BackupToDirectoryConfig struct {
	BackupConfig
	// FileSizeLimit is the maximum size of each backup file in bytes.
	// If FileSizeLimit is 0, backup file size is unbounded.
	// If non-zero, backup files will be split into multiple files if their size exceeds this limit.
	// If non-zero, FileSizeLimit must be greater than or equal to 1MB.
	// FileSizeLimit is not a strict limit, the actual file size may exceed this limit by a small amount.
	FileSizeLimit int64
}

// NewBackupToDirectoryConfig returns a new BackupToDirectoryConfig with default values.
func NewBackupToDirectoryConfig() *BackupToDirectoryConfig {
	return &BackupToDirectoryConfig{
		BackupConfig: *NewBackupConfig(),
	}
}

func (c *BackupToDirectoryConfig) validate() error {
	if c.FileSizeLimit > 0 && c.FileSizeLimit < 1024*1024 {
		return fmt.Errorf("file size limit must be 0 for no limit, or at least 1MB, got %d", c.FileSizeLimit)
	}

	if c.FileSizeLimit < 0 {
		return fmt.Errorf("file size limit must not be negative, got %d", c.FileSizeLimit)
	}

	return c.BackupConfig.validate()
}

// BackupToDirectory starts a backup operation
// that writes data to a local directory.
// config.Parallel determines the number of files to write concurrently.
// ctx can be used to cancel the backup operation.
// directory is the directory to write the backup data to.
// config is the configuration for the backup operation.
func (c *Client) BackupToDirectory(ctx context.Context,
	directory string, config *BackupToDirectoryConfig) (*BackupToDirectoryHandler, error) {
	if config == nil {
		config = NewBackupToDirectoryConfig()
	}

	// copy the policies so we don't modify the original
	infoPolicy := c.getUsableInfoPolicy(config.InfoPolicy)
	config.InfoPolicy = &infoPolicy

	scanPolicy := c.getUsableScanPolicy(config.ScanPolicy)
	config.ScanPolicy = &scanPolicy

	if err := config.validate(); err != nil {
		return nil, err
	}

	handler := newBackupToDirectoryHandler(config, c.aerospikeClient, directory, c.logger)
	handler.run(ctx)

	return handler, nil
}

// **** Restore ****

// DecoderFactory is used to specify the decoder with which to decode the backup data
// if nil, the default decoder factory will be used.
type DecoderFactory interface {
	CreateDecoder(src io.Reader) (encoding.Decoder, error)
}

// RestoreConfig contains configuration for the restore operation.
type RestoreConfig struct {
	// DecoderFactory is used to specify the decoder with which to decode the backup data
	// if nil, the default DecoderFactory will be used.
	DecoderFactory DecoderFactory
	// InfoPolicy applies to Aerospike Info requests made during backup and restore
	// If nil, the Aerospike client's default policy will be used.
	InfoPolicy *a.InfoPolicy
	// WritePolicy applies to Aerospike write operations made during backup and restore
	// If nil, the Aerospike client's default policy will be used.
	WritePolicy *a.WritePolicy
	// Parallel is the number of concurrent record writers to run against the Aerospike cluster.
	Parallel int
}

func (c *RestoreConfig) validate() error {
	if c.Parallel < MinParallel || c.Parallel > MaxParallel {
		return fmt.Errorf("parallel must be between 1 and 1024, got %d", c.Parallel)
	}

	return nil
}

// NewRestoreConfig returns a new RestoreConfig with default values.
func NewRestoreConfig() *RestoreConfig {
	return &RestoreConfig{
		Parallel:       4,
		DecoderFactory: defaultDecoderFactory,
	}
}

// Restore starts a restore operation from a set of io.readers.
// ctx can be used to cancel the restore operation.
// readers is a set of io.readers to read the backup data from.
// config is the configuration for the restore operation.
func (c *Client) Restore(ctx context.Context, readers []io.Reader, config *RestoreConfig) (*RestoreHandler, error) {
	if config == nil {
		config = NewRestoreConfig()
	}

	// copy the policies so we don't modify the original
	infoPolicy := c.getUsableInfoPolicy(config.InfoPolicy)
	config.InfoPolicy = &infoPolicy

	writePolicy := c.getUsableWritePolicy(config.WritePolicy)
	config.WritePolicy = &writePolicy

	if err := config.validate(); err != nil {
		return nil, err
	}

	handler := newRestoreHandler(config, c.aerospikeClient, readers, c.logger)
	handler.run(ctx)

	return handler, nil
}

// RestoreFromDirectoryConfig contains configuration for the restore from directory operation.
type RestoreFromDirectoryConfig struct {
	RestoreConfig
}

// NewRestoreFromDirectoryConfig returns a new RestoreFromDirectoryConfig with default values.
func NewRestoreFromDirectoryConfig() *RestoreFromDirectoryConfig {
	return &RestoreFromDirectoryConfig{
		RestoreConfig: *NewRestoreConfig(),
	}
}

// RestoreFromDirectory starts a restore operation
// that reads data from a local directory.
// The backup data may be in a single file or multiple files.
// config.Parallel determines the number of files to read concurrently.
// All backup files in the directory must have been generated by the same backup operation.
// ctx can be used to cancel the restore operation.
// directory is the directory to read the backup data from.
// config is the configuration for the restore operation.
func (c *Client) RestoreFromDirectory(ctx context.Context,
	directory string, config *RestoreFromDirectoryConfig) (*RestoreFromDirectoryHandler, error) {
	if config == nil {
		config = NewRestoreFromDirectoryConfig()
	}

	// copy the policies so we don't modify the original
	infoPolicy := c.getUsableInfoPolicy(config.InfoPolicy)
	config.InfoPolicy = &infoPolicy

	writePolicy := c.getUsableWritePolicy(config.WritePolicy)
	config.WritePolicy = &writePolicy

	if err := config.validate(); err != nil {
		return nil, err
	}

	handler := newRestoreFromDirectoryHandler(config, c.aerospikeClient, directory, c.logger)
	handler.run(ctx)

	return handler, nil
}
