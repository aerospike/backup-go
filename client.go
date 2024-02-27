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
// 	backupHandler, err := backupClient.BackupToWriter(writers, nil)
// 	if err != nil {
// 		// handle error
//  }
// 	// use the backupHandler to wait for the backup operation to finish
// 	// err = backupHandler.Wait()

type Client struct {
	aerospikeClient *a.Client
	config          *Config
}

// NewClient creates a new backuplib client
func NewClient(ac *a.Client, cc *Config) (*Client, error) {
	if cc == nil {
		cc = NewConfig()
	}

	if ac == nil {
		return nil, errors.New("aerospike client pointer is nil")
	}

	return &Client{
		aerospikeClient: ac,
		config:          cc,
	}, nil
}

// getUsablePolicy returns the policies to be used for the backup and restore operations
// If the input policies are nil, the client's default policies will be used
// If the client's default policies are nil, the aerospike client's default policies will be used
func (c *Client) getUsablePolicy(p *Policies) *Policies {
	policies := p
	if policies == nil {
		policies = c.config.Policies
	}
	if policies == nil {
		policies = &Policies{}
	}

	if policies.InfoPolicy == nil {
		policies.InfoPolicy = c.aerospikeClient.DefaultInfoPolicy
	}

	if policies.WritePolicy == nil {
		policies.WritePolicy = c.aerospikeClient.DefaultWritePolicy
	}

	if policies.ScanPolicy == nil {
		policies.ScanPolicy = c.aerospikeClient.DefaultScanPolicy
	}

	return policies
}

// Backup starts a backup operation to a set of io.writers
func (c *Client) Backup(ctx context.Context, writers []io.Writer, config *BackupConfig) (*BackupHandler, error) {
	if config == nil {
		config = NewBackupConfig()
	}
	config.Policies = c.getUsablePolicy(config.Policies)

	if err := config.validate(); err != nil {
		return nil, err
	}

	handler := newBackupHandler(config, c.aerospikeClient, writers)
	handler.run(ctx, writers)

	return handler, nil
}

// Restore starts a restore operation from a set of io.readers
func (c *Client) Restore(ctx context.Context, readers []io.Reader, config *RestoreConfig) (*RestoreHandler, error) {
	if config == nil {
		config = NewRestoreConfig()
	}
	config.Policies = c.getUsablePolicy(config.Policies)

	if err := config.validate(); err != nil {
		return nil, err
	}

	handler := newRestoreHandler(config, c.aerospikeClient, readers)
	handler.run(ctx, readers)

	return handler, nil
}
