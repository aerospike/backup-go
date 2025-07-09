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

package config

import (
	"fmt"
	"log/slog"
	"path"
	"runtime"
	"strconv"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/tools-common-go/client"
)

// MaxRack max number of racks that can exist.
const MaxRack = 1000000

// BackupParams contain backup parameters.
type BackupParams struct {
	App          *models.App
	ClientConfig *client.AerospikeConfig
	ClientPolicy *models.ClientPolicy
	Backup       *models.Backup
	BackupXDR    *models.BackupXDR
	Common       *models.Common
	Compression  *models.Compression
	Encryption   *models.Encryption
	SecretAgent  *models.SecretAgent
	AwsS3        *models.AwsS3
	GcpStorage   *models.GcpStorage
	AzureBlob    *models.AzureBlob
}

// IsXDR determines if the backup configuration is an XDR backup by checking if BackupXDR is non-nil and Backup is nil.
func (p *BackupParams) IsXDR() bool {
	return p.BackupXDR != nil && p.Backup == nil
}

// IsContinue determines if the backup configuration is a continue backup
// by checking if Backup is non-nil and Continue is non-empty.
func (p *BackupParams) IsContinue() bool {
	return p.Backup != nil && p.Backup.Continue != ""
}

// IsStopXDR checks if the backup operation should stop XDR by verifying that BackupXDR is non-nil and StopXDR is true.
func (p *BackupParams) IsStopXDR() bool {
	return p.BackupXDR != nil && p.BackupXDR.StopXDR
}

// IsUnblockMRT checks if the backup operation should unblock MRT writes
// by verifying that BackupXDR is non-nil and UnblockMRT is true.
func (p *BackupParams) IsUnblockMRT() bool {
	return p.BackupXDR != nil && p.BackupXDR.UnblockMRT
}

// SkipWriterInit checks if the backup operation should skip writer initialization
// by verifying that Backup is non-nil and Estimate is false.
func (p *BackupParams) SkipWriterInit() bool {
	if p.Backup != nil {
		return !p.Backup.Estimate
	}

	return true
}

// NewBackupConfigs creates and returns a new ConfigBackup and ConfigBackupXDR object,
// initialized with given backup parameters.
// This function sets various backup parameters including namespace, file limits, parallelism options, bandwidth,
// compression, encryption, and partition filters. It returns an error if any validation or parsing fails.
// If the backup is an XDR backup, it will return a ConfigBackupXDR object.
// Otherwise, it will return a ConfigBackup object.
func NewBackupConfigs(params *BackupParams, logger *slog.Logger,
) (*backup.ConfigBackup, *backup.ConfigBackupXDR, error) {
	var (
		backupConfig    *backup.ConfigBackup
		backupXDRConfig *backup.ConfigBackupXDR
		err             error
	)

	logger.Info("initializing backup config")

	switch params.IsXDR() {
	case false:
		backupConfig, err = NewBackupConfig(params)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to map backup config: %w", err)
		}

		logger.Info("initialized scan backup config",
			slog.String("namespace", backupConfig.Namespace),
			slog.String("encryption", params.Encryption.Mode),
			slog.Int("compression", params.Compression.Level),
			slog.String("filters", params.Backup.PartitionList),
			slog.Any("nodes", backupConfig.NodeList),
			slog.Any("sets", backupConfig.SetList),
			slog.Any("bins", backupConfig.BinList),
			slog.Any("rack", backupConfig.RackList),
			slog.Any("parallel_node", backupConfig.ParallelNodes),
			slog.Any("parallel_read", backupConfig.ParallelRead),
			slog.Any("parallel_write", backupConfig.ParallelWrite),
			slog.Bool("no_records", backupConfig.NoRecords),
			slog.Bool("no_indexes", backupConfig.NoIndexes),
			slog.Bool("no_udfs", backupConfig.NoUDFs),
			slog.Int("records_per_second", backupConfig.RecordsPerSecond),
			slog.Int64("bandwidth", backupConfig.Bandwidth),
			slog.Uint64("file_limit", backupConfig.FileLimit),
			slog.Bool("compact", backupConfig.Compact),
			slog.Bool("not_ttl_only", backupConfig.NoTTLOnly),
			slog.String("state_file", backupConfig.StateFile),
			slog.Bool("continue", backupConfig.Continue),
			slog.Int64("page_size", backupConfig.PageSize),
			slog.String("output_prefix", backupConfig.OutputFilePrefix),
		)
	case true:
		backupXDRConfig = NewBackupXDRConfig(params)

		// On xdr backup we backup only uds and indexes.
		backupConfig = backup.NewDefaultBackupConfig()

		backupConfig.NoRecords = true
		backupConfig.Namespace = backupXDRConfig.Namespace

		logger.Info("initialized xdr backup config",
			slog.String("namespace", backupXDRConfig.Namespace),
			slog.String("encryption", params.Encryption.Mode),
			slog.Int("compression", params.Compression.Level),
			slog.Any("parallel_write", backupXDRConfig.ParallelWrite),
			slog.Uint64("file_limit", backupXDRConfig.FileLimit),
			slog.String("dc", backupXDRConfig.DC),
			slog.String("local_address", backupXDRConfig.LocalAddress),
			slog.Int("local_port", backupXDRConfig.LocalPort),
			slog.String("rewind", backupXDRConfig.Rewind),
			slog.Int("max_throughput", backupXDRConfig.MaxThroughput),
			slog.Duration("read_timeout", backupXDRConfig.ReadTimeout),
			slog.Duration("write_timeout", backupXDRConfig.WriteTimeout),
			slog.Int("result_queue_size", backupXDRConfig.ResultQueueSize),
			slog.Int("ack_queue_size", backupXDRConfig.AckQueueSize),
			slog.Int("max_connections", backupXDRConfig.MaxConnections),
		)
	}

	return backupConfig, backupXDRConfig, nil
}

// NewBackupConfig initializes and returns a configured instance of ConfigBackup based on the provided params.
// This function sets various backup parameters including namespace, file limits, parallelism options, bandwidth,
// compression, encryption, and partition filters. It returns an error if any validation or parsing fails.
func NewBackupConfig(params *BackupParams) (*backup.ConfigBackup, error) {
	c := backup.NewDefaultBackupConfig()
	c.Namespace = params.Common.Namespace
	c.SetList = SplitByComma(params.Common.SetList)
	c.BinList = SplitByComma(params.Common.BinList)
	c.NoRecords = params.Common.NoRecords
	c.NoIndexes = params.Common.NoIndexes
	c.RecordsPerSecond = params.Common.RecordsPerSecond
	c.FileLimit = params.Backup.FileLimit
	c.NoUDFs = params.Common.NoUDFs
	// The original backup tools have a single parallelism configuration property.
	// We may consider splitting the configuration in the future.
	c.ParallelWrite = params.Common.Parallel
	c.ParallelRead = params.Common.Parallel
	// As we set --nice in MiB we must convert it to bytes
	c.Bandwidth = params.Common.Nice * 1024 * 1024
	c.Compact = params.Backup.Compact
	c.NoTTLOnly = params.Backup.NoTTLOnly
	c.OutputFilePrefix = params.Backup.OutputFilePrefix
	c.MetricsEnabled = true

	if params.Backup.RackList != "" {
		list, err := ParseRacks(params.Backup.RackList)
		if err != nil {
			return nil, err
		}

		c.RackList = list
	}

	if params.Backup.Continue != "" {
		c.StateFile = path.Join(params.Common.Directory, params.Backup.Continue)
		c.Continue = true
		c.PageSize = params.Backup.ScanPageSize
	}

	if params.Backup.StateFileDst != "" {
		c.StateFile = path.Join(params.Common.Directory, params.Backup.StateFileDst)
		c.PageSize = params.Backup.ScanPageSize
	}

	// Overwrite partitions if we use nodes.
	if params.Backup.ParallelNodes || params.Backup.NodeList != "" {
		c.ParallelNodes = params.Backup.ParallelNodes
		c.NodeList = SplitByComma(params.Backup.NodeList)
	}

	pf, err := mapPartitionFilter(params.Backup, params.Common)
	if err != nil {
		return nil, err
	}

	if err := validatePartitionFilters(pf); err != nil {
		return nil, err
	}

	c.PartitionFilters = pf

	sp, err := newScanPolicy(params.Backup, params.Common)
	if err != nil {
		return nil, err
	}

	c.ScanPolicy = sp
	c.CompressionPolicy = newCompressionPolicy(params.Compression)
	c.EncryptionPolicy = newEncryptionPolicy(params.Encryption)
	c.SecretAgentConfig = newSecretAgentConfig(params.SecretAgent)

	if params.Backup.ModifiedBefore != "" {
		modBeforeTime, err := parseLocalTimeToUTC(params.Backup.ModifiedBefore)
		if err != nil {
			return nil, fmt.Errorf("failed to parse modified before date: %w", err)
		}

		c.ModBefore = &modBeforeTime
	}

	if params.Backup.ModifiedAfter != "" {
		modAfterTime, err := parseLocalTimeToUTC(params.Backup.ModifiedAfter)
		if err != nil {
			return nil, fmt.Errorf("failed to parse modified after date: %w", err)
		}

		c.ModAfter = &modAfterTime
	}

	c.InfoRetryPolicy = mapRetryPolicy(
		params.Backup.InfoRetryIntervalMilliseconds,
		params.Backup.InfoRetriesMultiplier,
		params.Backup.InfoMaxRetries,
	)

	return c, nil
}

// NewBackupXDRConfig creates a ConfigBackupXDR instance based on the provided backup parameters.
func NewBackupXDRConfig(params *BackupParams) *backup.ConfigBackupXDR {
	parallelWrite := runtime.NumCPU()
	if params.BackupXDR.ParallelWrite > 0 {
		parallelWrite = params.BackupXDR.ParallelWrite
	}

	c := &backup.ConfigBackupXDR{
		InfoPolicy:        aerospike.NewInfoPolicy(),
		EncryptionPolicy:  newEncryptionPolicy(params.Encryption),
		CompressionPolicy: newCompressionPolicy(params.Compression),
		SecretAgentConfig: newSecretAgentConfig(params.SecretAgent),
		EncoderType:       backup.EncoderTypeASBX,
		FileLimit:         params.BackupXDR.FileLimit,
		ParallelWrite:     parallelWrite,
		DC:                params.BackupXDR.DC,
		LocalAddress:      params.BackupXDR.LocalAddress,
		LocalPort:         params.BackupXDR.LocalPort,
		Namespace:         params.BackupXDR.Namespace,
		Rewind:            params.BackupXDR.Rewind,
		TLSConfig:         nil,
		ReadTimeout:       time.Duration(params.BackupXDR.ReadTimeoutMilliseconds) * time.Millisecond,
		WriteTimeout:      time.Duration(params.BackupXDR.WriteTimeoutMilliseconds) * time.Millisecond,
		ResultQueueSize:   params.BackupXDR.ResultQueueSize,
		AckQueueSize:      params.BackupXDR.AckQueueSize,
		MaxConnections:    params.BackupXDR.MaxConnections,
		InfoPolingPeriod:  time.Duration(params.BackupXDR.InfoPolingPeriodMilliseconds) * time.Millisecond,
		StartTimeout:      time.Duration(params.BackupXDR.StartTimeoutMilliseconds) * time.Millisecond,
		InfoRetryPolicy: mapRetryPolicy(
			params.BackupXDR.InfoRetryIntervalMilliseconds,
			params.BackupXDR.InfoRetriesMultiplier,
			params.BackupXDR.InfoMaxRetries,
		),
		MaxThroughput:  params.BackupXDR.MaxThroughput,
		Forward:        params.BackupXDR.Forward,
		MetricsEnabled: true,
	}

	return c
}

// ParseRacks parses a comma-separated string of rack IDs into a slice of positive integers.
// Returns an error if any ID is invalid or exceeds the allowed maximum limit.
func ParseRacks(racks string) ([]int, error) {
	racksStringSlice := SplitByComma(racks)
	racksIntSlice := make([]int, 0, len(racksStringSlice))

	for i := range racksStringSlice {
		rackID, err := strconv.Atoi(racksStringSlice[i])
		if err != nil {
			return nil, fmt.Errorf("failed to parse racks: %w", err)
		}

		if rackID < 0 {
			return nil, fmt.Errorf("rack id %d invalid, should be positive number", rackID)
		}

		if rackID > MaxRack {
			return nil, fmt.Errorf("rack id %d invalid, should not exceed %d", rackID, MaxRack)
		}

		racksIntSlice = append(racksIntSlice, rackID)
	}

	return racksIntSlice, nil
}
