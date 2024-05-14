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
	"time"

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
//		backupClient, err := backup.NewClient(asc, "id", nil, backupCFG)	// create a backup client
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

// PartitionRangeAll return partition range containing all partitions.
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
	// Only include records that last changed before the given time (optional).
	ModBefore *time.Time
	// Only include records that last changed after the given time (optional).
	ModAfter  *time.Time
	Namespace string
	// SetList is the Aerospike set to backup (optional, given an empty list, all sets will be backed up).
	SetList []string
	// The list of backup bin names (optional, given an empty list, all bins will be backed up)
	BinList []string
	// Partitions specifies the Aerospike partitions to backup.
	Partitions PartitionRange
	// Parallel is the number of concurrent scans to run against the Aerospike cluster.
	Parallel int
	// Don't backup any records.
	NoRecords bool
	// Don't backup any secondary indexes.
	NoIndexes bool
	// Don't backup any UDFs.
	NoUDFs bool
}

func (c *BackupConfig) validate() error {
	if c.Parallel < MinParallel || c.Parallel > MaxParallel {
		return fmt.Errorf("parallel must be between 1 and 1024, got %d", c.Parallel)
	}

	if c.ModBefore != nil && c.ModAfter != nil && !c.ModBefore.After(*c.ModAfter) {
		return errors.New("modified before should be strictly greater than modified after")
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
		Namespace:      "test",
		EncoderFactory: defaultEncoderFactory,
	}
}

// Backup starts a backup operation
// that writes data to a provided writer.
// config.Parallel determines the number of files to write concurrently.
// ctx can be used to cancel the backup operation.
// config is the configuration for the backup operation.
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
	// Namespace details for the restore operation.
	// By default, the data is restored to the namespace from which it was taken.
	Namespace *RestoreNamespace `json:"namespace,omitempty"`
	// The sets to restore (optional, given an empty list, all sets will be restored).
	SetList []string
	// The bins to restore (optional, given an empty list, all bins will be restored).
	BinList []string
	// Parallel is the number of concurrent record writers to run against the Aerospike cluster.
	Parallel int
	// RecordsPerSecond limits restore records per second (rps) rate.
	// Will not apply rps limit if RecordsPerSecond is zero (default).
	RecordsPerSecond int
	// Don't restore any records.
	NoRecords bool
	// Don't restore any secondary indexes.
	NoIndexes bool
	// Don't restore any UDFs.
	NoUDFs bool
}

// RestoreNamespace specifies an alternative namespace name for the restore
// operation, where Source is the original namespace name and Destination is
// the namespace name to which the backup data is to be restored.
//
// @Description RestoreNamespace specifies an alternative namespace name for the restore
// @Description operation.
type RestoreNamespace struct {
	// Original namespace name.
	Source *string `json:"source,omitempty" example:"source-ns" validate:"required"`
	// Destination namespace name.
	Destination *string `json:"destination,omitempty" example:"destination-ns" validate:"required"`
}

// Validate validates the restore namespace.
func (n *RestoreNamespace) Validate() error {
	if n.Source == nil {
		return fmt.Errorf("source namespace is not specified")
	}

	if n.Destination == nil {
		return fmt.Errorf("destination namespace is not specified")
	}

	return nil
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

// Restore starts a restore operation
// that reads data from given readers.
// The backup data may be in a single file or multiple files.
// config.Parallel determines the number of files to read concurrently.
// ctx can be used to cancel the restore operation.
// directory is the directory to read the backup data from.
// config is the configuration for the restore operation.
// readerFactory provides readers with access to backup data.
func (c *Client) Restore(ctx context.Context, config *RestoreConfig, readerFactory ReaderFactory,
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

	handler := newRestoreHandler(config, c.aerospikeClient, c.logger, readerFactory)
	handler.run(ctx)

	return handler, nil
}
