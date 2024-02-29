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

package backuplib

import (
	"context"
	"errors"
	"io"

	a "github.com/aerospike/aerospike-client-go/v7"
)

// Client is the main entry point for the backuplib package
// It wraps an aerospike client and provides methods to start backup and restore operations
// It contains a Config object that can be used to set default policies for the backup and restore operations
// If the Policies field is nil, the aerospike client's default policies will be used
// These policies will be used as defaults for any backup and restore operations started by the client
// Example usage:
// 	asc, aerr := a.NewClientWithPolicy(...)	// create an aerospike client
// 	if aerr != nil {
// 		// handle error
// 	}
// 	backupCFG := backuplib.NewConfig()	// create a backuplib config
// 	backupClient, err := backuplib.NewClient(asc, backupCFG)	// create a backuplib client
// 	if err != nil {
// 		// handle error
// 	}
// 	// use the backup client to start backup and restore operations
//  ctx := context.Background()
// 	backupHandler, err := backupClient.Backup(ctx, writers, nil)
// 	if err != nil {
// 		// handle error
//  }
//  // optionally, check the stats of the backup operation
// 	stats := backupHandler.Stats()
// 	// use the backupHandler to wait for the backup operation to finish
//  ctx := context.Background()
// 	// err = backupHandler.Wait(ctx)

type Client struct {
	aerospikeClient *a.Client
	config          *Config
}

// NewClient creates a new backuplib client
// ac is the aerospike client to use for backup and restore operations
// config is the configuration for the backuplib client
func NewClient(ac *a.Client, config *Config) (*Client, error) {
	if config == nil {
		config = NewConfig()
	}

	if ac == nil {
		return nil, errors.New("aerospike client pointer is nil")
	}

	return &Client{
		aerospikeClient: ac,
		config:          config,
	}, nil
}

func (c *Client) getUsableInfoPolicy(p *a.InfoPolicy) *a.InfoPolicy {
	if p == nil {
		p = c.aerospikeClient.DefaultInfoPolicy
	}

	return p
}

func (c *Client) getUsableWritePolicy(p *a.WritePolicy) *a.WritePolicy {
	if p == nil {
		p = c.aerospikeClient.DefaultWritePolicy
	}

	return p
}

func (c *Client) getUsableScanPolicy(p *a.ScanPolicy) *a.ScanPolicy {
	if p == nil {
		p = c.aerospikeClient.DefaultScanPolicy
	}

	return p
}

// Backup starts a backup operation to a set of io.writers
// ctx can be used to cancel the backup operation
// writers is a set of io.writers to write the backup data to
// config is the configuration for the backup operation
func (c *Client) Backup(ctx context.Context, writers []io.Writer, config *BackupConfig) (*BackupHandler, error) {
	if config == nil {
		config = NewBackupConfig()
	}

	config.InfoPolicy = c.getUsableInfoPolicy(config.InfoPolicy)
	config.ScanPolicy = c.getUsableScanPolicy(config.ScanPolicy)

	if err := config.validate(); err != nil {
		return nil, err
	}

	handler := newBackupHandler(config, c.aerospikeClient, writers)
	handler.run(ctx, writers)

	return handler, nil
}

// Restore starts a restore operation from a set of io.readers
// ctx can be used to cancel the restore operation
// readers is a set of io.readers to read the backup data from
// config is the configuration for the restore operation
func (c *Client) Restore(ctx context.Context, readers []io.Reader, config *RestoreConfig) (*RestoreHandler, error) {
	if config == nil {
		config = NewRestoreConfig()
	}

	config.InfoPolicy = c.getUsableInfoPolicy(config.InfoPolicy)
	config.WritePolicy = c.getUsableWritePolicy(config.WritePolicy)

	if err := config.validate(); err != nil {
		return nil, err
	}

	handler := newRestoreHandler(config, c.aerospikeClient, readers)
	handler.run(ctx, readers)

	return handler, nil
}
