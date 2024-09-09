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

package app

import (
	"fmt"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/internal/models"
)

func mapBackupConfig(b *models.Backup) (*backup.BackupConfig, error) {
	if b.Namespace == "" {
		return nil, fmt.Errorf("namespace is required")
	}

	c := backup.NewDefaultBackupConfig()
	c.Namespace = b.Namespace
	c.SetList = b.SetList
	c.BinList = b.BinList
	c.NoRecords = b.NoRecords
	c.NoIndexes = b.NoIndexes
	c.RecordsPerSecond = b.RecordsPerSecond
	c.FileLimit = b.FileLimit
	c.AfterDigest = b.AfterDigest
	c.Parallel = b.Parallel

	sp, err := mapScanPolicy(b)
	if err != nil {
		return nil, err
	}

	c.ScanPolicy = sp

	if b.ModifiedBefore != "" {
		modBeforeTime, err := time.Parse("2006-01-02_15:04:05", b.ModifiedBefore)
		if err != nil {
			return nil, fmt.Errorf("failed to parse modified before date: %v", err)
		}

		c.ModBefore = &modBeforeTime
	}

	if b.ModifiedAfter != "" {
		modAfterTime, err := time.Parse("2006-01-02_15:04:05", b.ModifiedAfter)
		if err != nil {
			return nil, fmt.Errorf("failed to parse modified after date: %v", err)
		}

		c.ModAfter = &modAfterTime
	}

	if len(b.SetList) > 0 {
		c.SetList = b.SetList
	}

	return c, nil
}

func mapCompressionPolicy(c *models.Compression) *backup.CompressionPolicy {
	if c.Mode == "" {
		return nil
	}

	return &backup.CompressionPolicy{
		Mode:  strings.ToUpper(c.Mode),
		Level: c.Level,
	}
}

func mapEncryptionPolicy(e *models.Encryption) *backup.EncryptionPolicy {
	if e.Mode == "" {
		return nil
	}

	p := &backup.EncryptionPolicy{
		Mode: strings.ToUpper(e.Mode),
	}

	if e.KeyFile != "" {
		p.KeyFile = &e.KeyFile
	}

	if e.KeyEnv != "" {
		p.KeyEnv = &e.KeyEnv
	}

	if e.KeySecret != "" {
		p.KeySecret = &e.KeySecret
	}

	return p
}

func mapSecretAgentConfig(s *models.SecretAgent) *backup.SecretAgentConfig {
	if s.Address == "" {
		return nil
	}

	c := &backup.SecretAgentConfig{}
	c.Address = &s.Address

	if s.ConnectionType != "" {
		c.ConnectionType = &s.ConnectionType
	}

	if s.Port != 0 {
		c.Port = &s.Port
	}

	if s.TimeoutMillisecond != 0 {
		c.TimeoutMillisecond = &s.TimeoutMillisecond
	}

	if s.CaFile != "" {
		c.CaFile = &s.CaFile
	}

	if s.IsBase64 {
		c.IsBase64 = &s.IsBase64
	}

	return c
}

func mapScanPolicy(b *models.Backup) (*aerospike.ScanPolicy, error) {
	p := &aerospike.ScanPolicy{}
	p.MaxRecords = b.MaxRecords
	p.MaxRetries = b.MaxRetries
	p.SleepBetweenRetries = time.Duration(b.SleepBetweenRetries) * time.Millisecond
	p.TotalTimeout = time.Duration(b.TotalTimeout) * time.Millisecond
	p.SocketTimeout = time.Duration(b.SocketTimeout) * time.Millisecond

	if b.NoBins {
		p.IncludeBinData = false
	}

	if b.FilterExpression != "" {
		exp, err := aerospike.ExpFromBase64(b.FilterExpression)
		if err != nil {
			return nil, fmt.Errorf("failed to parse filter expression: %v", err)
		}

		p.FilterExpression = exp
	}

	return p, nil
}
