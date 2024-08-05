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
//	asc, aerr := a.NewClientWithPolicy(...)	// create an aerospike client
//	if aerr != nil {
//		// handle error
//	}
//
//	backupClient, err := backup.NewClient(asc, "id", nil)	// create a backup client
//	if err != nil {
//		// handle error
//	}
//
//	writers, err := backup.NewWriterLocal("backups_folder", false)
//	if err != nil {
//		// handle error
//	}
//
//	// use the backup client to start backup and restore operations
//	ctx := context.Background()
//	backupHandler, err := backupClient.Backup(ctx, writers, nil)
//	if err != nil {
//		// handle error
//	}
//
//	// optionally, check the stats of the backup operation
//	stats := backupHandler.Stats()
//
//	// use the backupHandler to wait for the backup operation to finish
//	ctx := context.Background()
//	if err = backupHandler.Wait(ctx); err != nil {
//		// handle error
//	}
type Client struct {
	aerospikeClient *a.Client
	logger          *slog.Logger
	id              string
}

// NewClient creates a new backup client.
//   - ac is the aerospike client to use for backup and restore operations.
//   - id is an identifier for the client.
//   - logger is the logger that this client will log to.
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

func (c *Client) getUsableScanPolicy(p *a.ScanPolicy, maxRecords int64) a.ScanPolicy {
	if p == nil {
		p = c.aerospikeClient.DefaultScanPolicy
	}

	// Set maxRecords.
	if maxRecords != 0 {
		p.MaxRecords = maxRecords
	}

	return *p
}

// Backup starts a backup operation that writes data to a provided writer.
//   - ctx can be used to cancel the backup operation.
//   - config is the configuration for the backup operation.
//   - writer creates new writers for the backup operation.
func (c *Client) Backup(
	ctx context.Context,
	config *BackupConfig,
	writer Writer,
) (*BackupHandler, error) {
	if config == nil {
		return nil, fmt.Errorf("backup config required")
	}

	// copy the policies so we don't modify the original
	infoPolicy := c.getUsableInfoPolicy(config.InfoPolicy)
	config.InfoPolicy = &infoPolicy

	scanPolicy := c.getUsableScanPolicy(config.ScanPolicy, config.MaxRecords)
	config.ScanPolicy = &scanPolicy

	if err := config.validate(); err != nil {
		return nil, err
	}

	handler := newBackupHandler(config, c.aerospikeClient, c.logger, writer)
	handler.run(ctx)

	return handler, nil
}

// Restore starts a restore operation that reads data from given readers.
// The backup data may be in a single file or multiple files.
//   - ctx can be used to cancel the restore operation.
//   - config is the configuration for the restore operation.
//   - streamingReader provides readers with access to backup data.
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
