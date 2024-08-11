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
	"math/rand/v2"
	"strconv"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/internal/logging"
	"golang.org/x/sync/semaphore"
)

const (
	// MinParallel is the minimum number of workers to use during an operation.
	MinParallel = 1
	// MaxParallel is the maximum number of workers to use during an operation.
	MaxParallel = 1024
	// MaxPartitions is the maximum number of partitions in an Aerospike cluster.
	MaxPartitions = 4096
)

// AerospikeClient describes aerospike client interface for easy mocking.
//
//go:generate mockery --name AerospikeClient
type AerospikeClient interface {
	GetDefaultScanPolicy() *a.ScanPolicy
	GetDefaultInfoPolicy() *a.InfoPolicy
	GetDefaultWritePolicy() *a.WritePolicy
	Put(policy *a.WritePolicy, key *a.Key, bins a.BinMap) a.Error
	CreateComplexIndex(policy *a.WritePolicy, namespace string, set string, indexName string, binName string,
		indexType a.IndexType, indexCollectionType a.IndexCollectionType, ctx ...*a.CDTContext,
	) (*a.IndexTask, a.Error)
	DropIndex(policy *a.WritePolicy, namespace string, set string, indexName string) a.Error
	RegisterUDF(policy *a.WritePolicy, udfBody []byte, serverPath string, language a.Language,
	) (*a.RegisterTask, a.Error)
	BatchOperate(policy *a.BatchPolicy, records []a.BatchRecordIfc) a.Error
	Cluster() *a.Cluster
	ScanPartitions(scanPolicy *a.ScanPolicy, partitionFilter *a.PartitionFilter, namespace string,
		setName string, binNames ...string) (*a.Recordset, a.Error)
}

// Client is the main entry point for the backup package.
// It wraps an aerospike client and provides methods to start backup and restore operations.
// Example usage:
//
//	asc, aerr := a.NewClientWithPolicy(...)	// create an aerospike client
//	if aerr != nil {
//		// handle error
//	}
//
//	backupClient, err := backup.NewClient(asc, backup.WithID("id"))	// create a backup client
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
	aerospikeClient AerospikeClient
	logger          *slog.Logger
	scanLimiter     *semaphore.Weighted
	id              string
}

// ClientOpt is a functional option that allows configuring the [Client].
type ClientOpt func(*Client)

// WithID sets the ID for the Client.
func WithID(id string) ClientOpt {
	return func(c *Client) {
		c.id = id
	}
}

// WithLogger sets the logger for the [Client].
func WithLogger(logger *slog.Logger) ClientOpt {
	return func(c *Client) {
		c.logger = logger
	}
}

// WithScanLimiter sets the scan limiter for the [Client].
func WithScanLimiter(sem *semaphore.Weighted) ClientOpt {
	return func(c *Client) {
		c.scanLimiter = sem
	}
}

// NewClient creates a new backup client.
//   - ac is the aerospike client to use for backup and restore operations.
//
// Options:
//   - [WithID] to set an identifier for the client.
//   - [WithLogger] to set a logger that this client will log to.
//   - [WithScanLimiter] to set a semaphore that is used to limit number of
//     concurrent scans.
func NewClient(ac AerospikeClient, opts ...ClientOpt) (*Client, error) {
	if ac == nil {
		return nil, errors.New("aerospike client pointer is nil")
	}

	// Initialize the Client with default values
	client := &Client{
		aerospikeClient: ac,
		logger:          slog.Default(),
		id:              strconv.Itoa(rand.IntN(1000)),
	}

	// Apply all options to the Client
	for _, opt := range opts {
		opt(client)
	}

	// Further customization after applying options
	client.logger = client.logger.WithGroup("backup")
	client.logger = logging.WithClient(client.logger, client.id)

	return client, nil
}

func (c *Client) getUsableInfoPolicy(p *a.InfoPolicy) a.InfoPolicy {
	if p == nil {
		p = c.aerospikeClient.GetDefaultInfoPolicy()
	}

	return *p
}

func (c *Client) getUsableWritePolicy(p *a.WritePolicy) a.WritePolicy {
	if p == nil {
		p = c.aerospikeClient.GetDefaultWritePolicy()
	}

	return *p
}

func (c *Client) getUsableScanPolicy(p *a.ScanPolicy) a.ScanPolicy {
	if p == nil {
		p = c.aerospikeClient.GetDefaultScanPolicy()
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

	scanPolicy := c.getUsableScanPolicy(config.ScanPolicy)
	config.ScanPolicy = &scanPolicy

	if err := config.validate(); err != nil {
		return nil, err
	}

	handler := newBackupHandler(ctx, config, c.aerospikeClient, c.logger, writer, c.scanLimiter)
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

	handler := newRestoreHandler(ctx, config, c.aerospikeClient, c.logger, streamingReader)
	handler.startAsync(ctx)

	return handler, nil
}
