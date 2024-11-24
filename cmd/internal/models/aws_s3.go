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

type AwsS3 struct {
	BucketName string
	Region     string
	Profile    string
	Endpoint   string
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

	return nil
}
