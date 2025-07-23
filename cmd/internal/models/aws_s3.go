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

//nolint:dupl //This file is not a duplication of azure_blob.
package models

import (
	"fmt"

	"github.com/aerospike/backup-go"
)

// AwsS3 represents the configuration for AWS S3 storage integration.
type AwsS3 struct {
	BucketName      string
	Region          string
	Profile         string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string

	StorageClass        string
	AccessTier          string
	RestorePollDuration int64

	RetryMaxAttempts       int
	RetryMaxBackoffSeconds int
	RetryBackoffSeconds    int

	ChunkSize int
}

// LoadSecrets tries to load field values from secret agent.
func (a *AwsS3) LoadSecrets(cfg *backup.SecretAgentConfig) error {
	var err error

	a.BucketName, err = backup.ParseSecret(cfg, a.BucketName)
	if err != nil {
		return fmt.Errorf("failed to load bucket name from secret agent: %w", err)
	}

	a.Region, err = backup.ParseSecret(cfg, a.Region)
	if err != nil {
		return fmt.Errorf("failed to load region from secret agent: %w", err)
	}

	a.Profile, err = backup.ParseSecret(cfg, a.Profile)
	if err != nil {
		return fmt.Errorf("failed to load profile from secret agent: %w", err)
	}

	a.Endpoint, err = backup.ParseSecret(cfg, a.Endpoint)
	if err != nil {
		return fmt.Errorf("failed to load endpoint from secret agent: %w", err)
	}

	a.AccessKeyID, err = backup.ParseSecret(cfg, a.AccessKeyID)
	if err != nil {
		return fmt.Errorf("failed to load access key id from secret agent: %w", err)
	}

	a.SecretAccessKey, err = backup.ParseSecret(cfg, a.SecretAccessKey)
	if err != nil {
		return fmt.Errorf("failed to load secret access key from secret agent: %w", err)
	}

	a.StorageClass, err = backup.ParseSecret(cfg, a.StorageClass)
	if err != nil {
		return fmt.Errorf("failed to load storage class from secret agent: %w", err)
	}

	a.AccessTier, err = backup.ParseSecret(cfg, a.AccessTier)
	if err != nil {
		return fmt.Errorf("failed to load access tier key from secret agent: %w", err)
	}

	return nil
}

// Validate internal validation for struct params.
func (a *AwsS3) Validate() error {
	if a.BucketName == "" {
		return fmt.Errorf("bucket name is required")
	}

	if a.RetryMaxAttempts < 0 {
		return fmt.Errorf("retry maximum attempts must be non-negative")
	}

	if a.RetryMaxBackoffSeconds < 0 {
		return fmt.Errorf("retry max backoff must be non-negative")
	}

	if a.RetryBackoffSeconds < 0 {
		return fmt.Errorf("retry backoff must be non-negative")
	}

	if a.ChunkSize < 0 {
		return fmt.Errorf("chunk size must be non-negative")
	}

	if a.RestorePollDuration < 1 {
		return fmt.Errorf("restore poll duration can't be less than 1")
	}

	return nil
}
