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
	bModels "github.com/aerospike/backup-go/models"
)

func mapBackupConfig(
	backupParams *models.Backup,
	commonParams *models.Common,
	compression *models.Compression,
	encryption *models.Encryption,
	secretAgent *models.SecretAgent,
) (*backup.BackupConfig, error) {
	if commonParams.Namespace == "" {
		return nil, fmt.Errorf("namespace is required")
	}

	c := backup.NewDefaultBackupConfig()
	c.Namespace = commonParams.Namespace
	c.SetList = stringSplit(commonParams.SetList)
	c.BinList = stringSplit(commonParams.BinList)
	c.NoRecords = commonParams.NoRecords
	c.NoIndexes = commonParams.NoIndexes
	c.RecordsPerSecond = commonParams.RecordsPerSecond
	c.FileLimit = backupParams.FileLimit
	c.AfterDigest = backupParams.AfterDigest
	// The original backup tools have a single parallelism configuration property.
	// We may consider splitting the configuration in the future.
	c.ParallelWrite = commonParams.Parallel
	c.ParallelRead = commonParams.Parallel
	// As we set --nice in MiB we must convert it to bytes
	// TODO: make Bandwidth int64 to avoid overflow.
	c.Bandwidth = commonParams.Nice * 1024 * 1024
	c.Compact = backupParams.Compact
	c.NoTTLOnly = backupParams.NoTTLOnly

	// Overwrite partitions if we use nodes.
	if backupParams.ParallelNodes || backupParams.NodeList != "" {
		c.Partitions = backup.PartitionRange{}
		c.ParallelNodes = backupParams.ParallelNodes
		c.NodeList = stringSplit(backupParams.NodeList)
	}

	sp, err := mapScanPolicy(backupParams, commonParams)
	if err != nil {
		return nil, err
	}

	c.ScanPolicy = sp
	c.CompressionPolicy = mapCompressionPolicy(compression)
	c.EncryptionPolicy = mapEncryptionPolicy(encryption)
	c.SecretAgentConfig = mapSecretAgentConfig(secretAgent)

	if backupParams.ModifiedBefore != "" {
		modBeforeTime, err := time.Parse("2006-01-02_15:04:05", backupParams.ModifiedBefore)
		if err != nil {
			return nil, fmt.Errorf("failed to parse modified before date: %w", err)
		}

		c.ModBefore = &modBeforeTime
	}

	if backupParams.ModifiedAfter != "" {
		modAfterTime, err := time.Parse("2006-01-02_15:04:05", backupParams.ModifiedAfter)
		if err != nil {
			return nil, fmt.Errorf("failed to parse modified after date: %w", err)
		}

		c.ModAfter = &modAfterTime
	}

	return c, nil
}

func mapRestoreConfig(
	restoreParams *models.Restore,
	commonParams *models.Common,
	compression *models.Compression,
	encryption *models.Encryption,
	secretAgent *models.SecretAgent,
) (*backup.RestoreConfig, error) {
	if commonParams.Namespace == "" {
		return nil, fmt.Errorf("namespace is required")
	}

	c := backup.NewDefaultRestoreConfig()
	c.Namespace = mapRestoreNamespace(commonParams.Namespace)
	c.SetList = stringSplit(commonParams.SetList)
	c.BinList = stringSplit(commonParams.BinList)
	c.NoRecords = commonParams.NoRecords
	c.NoIndexes = commonParams.NoIndexes
	c.RecordsPerSecond = commonParams.RecordsPerSecond
	c.Parallel = commonParams.Parallel
	c.WritePolicy = mapWritePolicy(restoreParams, commonParams)
	c.InfoPolicy = mapInfoPolicy(restoreParams.TimeOut)
	// As we set --nice in MiB we must convert it to bytes
	// TODO: make Bandwidth int64 to avoid overflow.
	c.Bandwidth = commonParams.Nice * 1024 * 1024

	c.CompressionPolicy = mapCompressionPolicy(compression)
	c.EncryptionPolicy = mapEncryptionPolicy(encryption)
	c.SecretAgentConfig = mapSecretAgentConfig(secretAgent)
	c.RetryPolicy = mapRetryPolicy(restoreParams)

	return c, nil
}

func mapRestoreNamespace(n string) *backup.RestoreNamespaceConfig {
	nsArr := stringSplit(n)

	var source, destination string

	switch len(nsArr) {
	case 1:
		source, destination = nsArr[0], nsArr[0]
	case 2:
		source, destination = nsArr[0], nsArr[1]
	default:
		return nil
	}

	return &backup.RestoreNamespaceConfig{
		Source:      &source,
		Destination: &destination,
	}
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

func mapScanPolicy(b *models.Backup, c *models.Common) (*aerospike.ScanPolicy, error) {
	p := aerospike.NewScanPolicy()
	p.MaxRecords = b.MaxRecords
	p.MaxRetries = c.MaxRetries
	p.SleepBetweenRetries = time.Duration(b.SleepBetweenRetries) * time.Millisecond
	p.TotalTimeout = time.Duration(c.TotalTimeout) * time.Millisecond
	p.SocketTimeout = time.Duration(c.SocketTimeout) * time.Millisecond
	// If we selected racks we must set replica policy to aerospike.PREFER_RACK
	if b.PreferRacks != "" {
		p.ReplicaPolicy = aerospike.PREFER_RACK
	}

	if b.NoBins {
		p.IncludeBinData = false
	}

	if b.FilterExpression != "" {
		exp, err := aerospike.ExpFromBase64(b.FilterExpression)
		if err != nil {
			return nil, fmt.Errorf("failed to parse filter expression: %w", err)
		}

		p.FilterExpression = exp
	}

	return p, nil
}

func mapWritePolicy(r *models.Restore, c *models.Common) *aerospike.WritePolicy {
	p := aerospike.NewWritePolicy(0, 0)
	p.MaxRetries = c.MaxRetries
	p.TotalTimeout = time.Duration(c.TotalTimeout) * time.Millisecond
	p.SocketTimeout = time.Duration(c.SocketTimeout) * time.Millisecond
	p.RecordExistsAction = recordExistsAction(r.Replace, r.Uniq)
	p.GenerationPolicy = aerospike.EXPECT_GEN_GT

	if r.NoGeneration {
		p.GenerationPolicy = aerospike.NONE
	}

	return p
}

func recordExistsAction(replace, unique bool) aerospike.RecordExistsAction {
	switch {
	case replace:
		return aerospike.REPLACE
	case unique:
		return aerospike.CREATE_ONLY
	default:
		return aerospike.UPDATE
	}
}

// TODO: why no info policy timeout is set for backup in C tool?
func mapInfoPolicy(timeOut int64) *aerospike.InfoPolicy {
	p := aerospike.NewInfoPolicy()
	p.Timeout = time.Duration(timeOut) * time.Millisecond

	return p
}

func mapRetryPolicy(r *models.Restore) *bModels.RetryPolicy {
	// TODO: make constructor for bModels.RetryPolicy
	return &bModels.RetryPolicy{
		BaseTimeout: time.Duration(r.RetryBaseTimeout) * time.Millisecond,
		Multiplier:  r.RetryMultiplier,
		MaxRetries:  r.RetryMaxRetries,
	}
}

func stringSplit(s string) []string {
	if s == "" {
		return nil
	}

	return strings.Split(s, ",")
}
