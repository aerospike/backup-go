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

//nolint:dupl //This file is not a duplication of aws_s3.
package models

import (
	"fmt"

	"github.com/aerospike/backup-go"
)

// AzureBlob represents the configuration for Azure Blob storage integration.
type AzureBlob struct {
	// Account name + key auth
	AccountName string
	AccountKey  string
	// Azure Active directory
	TenantID     string
	ClientID     string
	ClientSecret string

	Endpoint      string
	ContainerName string

	AccessTier          string
	RestorePollDuration int64
}

// LoadSecrets tries to load field values from secret agent.
func (a *AzureBlob) LoadSecrets(cfg *backup.SecretAgentConfig) error {
	var err error

	a.AccountName, err = backup.ParseSecret(cfg, a.AccountName)
	if err != nil {
		return fmt.Errorf("failed to load account name from secret agent: %w", err)
	}

	a.AccountKey, err = backup.ParseSecret(cfg, a.AccountKey)
	if err != nil {
		return fmt.Errorf("failed to load account key from secret agent: %w", err)
	}

	a.TenantID, err = backup.ParseSecret(cfg, a.TenantID)
	if err != nil {
		return fmt.Errorf("failed to load tenant id from secret agent: %w", err)
	}

	a.ClientID, err = backup.ParseSecret(cfg, a.ClientID)
	if err != nil {
		return fmt.Errorf("failed to load client id from secret agent: %w", err)
	}

	a.ClientSecret, err = backup.ParseSecret(cfg, a.ClientSecret)
	if err != nil {
		return fmt.Errorf("failed to load client secret from secret agent: %w", err)
	}

	a.Endpoint, err = backup.ParseSecret(cfg, a.Endpoint)
	if err != nil {
		return fmt.Errorf("failed to load endpoint from secret agent: %w", err)
	}

	a.ContainerName, err = backup.ParseSecret(cfg, a.ContainerName)
	if err != nil {
		return fmt.Errorf("failed to load container name from secret agent: %w", err)
	}

	a.AccessTier, err = backup.ParseSecret(cfg, a.AccessTier)
	if err != nil {
		return fmt.Errorf("failed to load access tier key from secret agent: %w", err)
	}

	return nil
}
