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

package models

import (
	"fmt"

	"github.com/aerospike/backup-go"
)

// GcpStorage represents the configuration for GCP storage integration.
type GcpStorage struct {
	// Path to file containing Service Account JSON Key.
	KeyFile string
	// For GPC storage bucket is not part of the path as in S3.
	// So we should set it separately.
	BucketName string
	// Alternative url.
	// It is not recommended to use an alternate URL in a production environment.
	Endpoint string

	RetryMaxAttempts        int
	RetryBackoffMaxSeconds  int
	RetryBackoffInitSeconds int
	RetryBackoffMultiplier  float64

	ChunkSize int
}

// LoadSecrets tries to load field values from secret agent.
func (g *GcpStorage) LoadSecrets(cfg *backup.SecretAgentConfig) error {
	var err error

	g.KeyFile, err = backup.ParseSecret(cfg, g.KeyFile)
	if err != nil {
		return fmt.Errorf("failed to load key file from secret agent: %w", err)
	}

	g.BucketName, err = backup.ParseSecret(cfg, g.BucketName)
	if err != nil {
		return fmt.Errorf("failed to load bucket name from secret agent: %w", err)
	}

	g.Endpoint, err = backup.ParseSecret(cfg, g.Endpoint)
	if err != nil {
		return fmt.Errorf("failed to load endpoint from secret agent: %w", err)
	}

	return nil
}

// Validate internal validation for struct params.
func (g *GcpStorage) Validate() error {
	if g.BucketName == "" {
		return fmt.Errorf("bucket name is required")
	}

	if g.RetryMaxAttempts < 0 {
		return fmt.Errorf("retry maximum attempts must be non-negative")
	}

	if g.RetryBackoffMaxSeconds < 0 {
		return fmt.Errorf("retry max backoff must be non-negative")
	}

	if g.RetryBackoffInitSeconds < 0 {
		return fmt.Errorf("retry backoff must be non-negative")
	}

	if g.RetryBackoffMultiplier < 1 {
		return fmt.Errorf("retry backoff multiplier must be positive")
	}

	if g.ChunkSize < 0 {
		return fmt.Errorf("chunk size must be non-negative")
	}

	return nil
}
