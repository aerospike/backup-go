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
	AccountName string `yaml:"account-name,omitempty"`
	AccountKey  string `yaml:"account-key,omitempty"`
	// Azure Active directory
	TenantID     string `yaml:"tenant-id,omitempty"`
	ClientID     string `yaml:"client-id,omitempty"`
	ClientSecret string `yaml:"client-secret,omitempty"`

	Endpoint      string `yaml:"endpoint-override,omitempty"`
	ContainerName string `yaml:"container-name,omitempty"`

	AccessTier          string `yaml:"access-tier,omitempty"`
	RestorePollDuration int64  `yaml:"rehydrate-poll-duration,omitempty"`

	RetryMaxAttempts     int `yaml:"retry-max-attempts,omitempty"`
	RetryTimeoutSeconds  int `yaml:"retry-timeout,omitempty"`
	RetryDelaySeconds    int `yaml:"retry-delay,omitempty"`
	RetryMaxDelaySeconds int `yaml:"retry-max-delay,omitempty"`

	BlockSize int `yaml:"block-size,omitempty"`
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

// Validate internal validation for struct backup.
func (a *AzureBlob) Validate() error {
	if a.ContainerName == "" {
		return fmt.Errorf("container name is required")
	}

	if a.RetryMaxAttempts < 0 {
		return fmt.Errorf("retry maximum attempts must be non-negative")
	}

	if a.RetryTimeoutSeconds < 0 {
		return fmt.Errorf("retry try timeout must be non-negative")
	}

	if a.RetryDelaySeconds < 0 {
		return fmt.Errorf("retry delay must be non-negative")
	}

	if a.RetryMaxDelaySeconds < 0 {
		return fmt.Errorf("retry max delay must be non-negative")
	}

	if a.BlockSize < 0 {
		return fmt.Errorf("block size must be non-negative")
	}

	if a.RestorePollDuration < 1 {
		return fmt.Errorf("rehydrate poll duration can't be less than 1")
	}

	return nil
}
