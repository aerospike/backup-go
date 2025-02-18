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
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/internal/models"
	bModels "github.com/aerospike/backup-go/models"
)

var (
	//nolint:lll // The regexp is long.
	expPartitionRange  = regexp.MustCompile(`^([0-9]|[1-9][0-9]{1,3}|40[0-8][0-9]|409[0-5])\-([1-9]|[1-9][0-9]{1,3}|40[0-8][0-9]|409[0-6])$`)
	expPartitionID     = regexp.MustCompile(`^(409[0-6]|40[0-8]\d|[123]?\d{1,3}|0)$`)
	expPartitionDigest = regexp.MustCompile(`^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$`)
	// Time parsing expressions.
	expTimeOnly = regexp.MustCompile(`^\d{2}:\d{2}:\d{2}$`)
	expDateOnly = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	expDateTime = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}$`)
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
	c.SetList = splitByComma(commonParams.SetList)
	c.BinList = splitByComma(commonParams.BinList)
	c.NoRecords = commonParams.NoRecords
	c.NoIndexes = commonParams.NoIndexes
	c.RecordsPerSecond = commonParams.RecordsPerSecond
	c.FileLimit = backupParams.FileLimit
	c.NoUDFs = commonParams.NoUDFs
	// The original backup tools have a single parallelism configuration property.
	// We may consider splitting the configuration in the future.
	c.ParallelWrite = commonParams.Parallel
	c.ParallelRead = commonParams.Parallel
	// As we set --nice in MiB we must convert it to bytes
	c.Bandwidth = commonParams.Nice * 1024 * 1024
	c.Compact = backupParams.Compact
	c.NoTTLOnly = backupParams.NoTTLOnly
	c.OutputFilePrefix = backupParams.OutputFilePrefix

	if backupParams.Continue != "" {
		c.StateFile = backupParams.Continue
		c.Continue = true
		c.SyncPipelines = true
		c.PageSize = backupParams.ScanPageSize
	}

	if backupParams.StateFileDst != "" {
		c.StateFile = backupParams.StateFileDst
		c.SyncPipelines = true
		c.PageSize = backupParams.ScanPageSize
	}

	// Overwrite partitions if we use nodes.
	if backupParams.ParallelNodes || backupParams.NodeList != "" {
		c.ParallelNodes = backupParams.ParallelNodes
		c.NodeList = splitByComma(backupParams.NodeList)
	}

	pf, err := mapPartitionFilter(backupParams, commonParams)
	if err != nil {
		return nil, err
	}

	if err := validatePartitionFilters(pf); err != nil {
		return nil, err
	}

	c.PartitionFilters = pf

	sp, err := mapScanPolicy(backupParams, commonParams)
	if err != nil {
		return nil, err
	}

	c.ScanPolicy = sp
	c.CompressionPolicy = mapCompressionPolicy(compression)
	c.EncryptionPolicy = mapEncryptionPolicy(encryption)
	c.SecretAgentConfig = mapSecretAgentConfig(secretAgent)

	if backupParams.ModifiedBefore != "" {
		modBeforeTime, err := parseLocalTimeToUTC(backupParams.ModifiedBefore)
		if err != nil {
			return nil, fmt.Errorf("failed to parse modified before date: %w", err)
		}

		c.ModBefore = &modBeforeTime
	}

	if backupParams.ModifiedAfter != "" {
		modAfterTime, err := parseLocalTimeToUTC(backupParams.ModifiedAfter)
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
	c.SetList = splitByComma(commonParams.SetList)
	c.BinList = splitByComma(commonParams.BinList)
	c.NoRecords = commonParams.NoRecords
	c.NoIndexes = commonParams.NoIndexes
	c.NoUDFs = commonParams.NoUDFs
	c.RecordsPerSecond = commonParams.RecordsPerSecond
	c.Parallel = commonParams.Parallel
	c.WritePolicy = mapWritePolicy(restoreParams, commonParams)
	c.InfoPolicy = mapInfoPolicy(restoreParams.TimeOut)
	// As we set --nice in MiB we must convert it to bytes
	c.Bandwidth = commonParams.Nice * 1024 * 1024
	c.ExtraTTL = restoreParams.ExtraTTL
	c.IgnoreRecordError = restoreParams.IgnoreRecordError
	c.DisableBatchWrites = restoreParams.DisableBatchWrites
	c.BatchSize = restoreParams.BatchSize
	c.MaxAsyncBatches = restoreParams.MaxAsyncBatches

	c.CompressionPolicy = mapCompressionPolicy(compression)
	c.EncryptionPolicy = mapEncryptionPolicy(encryption)
	c.SecretAgentConfig = mapSecretAgentConfig(secretAgent)
	c.RetryPolicy = mapRetryPolicy(restoreParams)

	return c, nil
}

func mapRestoreNamespace(n string) *backup.RestoreNamespaceConfig {
	nsArr := splitByComma(n)

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

	return backup.NewCompressionPolicy(strings.ToUpper(c.Mode), c.Level)
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
	p.SendKey = true
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

func mapInfoPolicy(timeOut int64) *aerospike.InfoPolicy {
	p := aerospike.NewInfoPolicy()
	p.Timeout = time.Duration(timeOut) * time.Millisecond

	return p
}

func mapRetryPolicy(r *models.Restore) *bModels.RetryPolicy {
	return bModels.NewRetryPolicy(
		time.Duration(r.RetryBaseTimeout)*time.Millisecond,
		r.RetryMultiplier,
		r.RetryMaxRetries,
	)
}

func splitByComma(s string) []string {
	if s == "" {
		return nil
	}

	return strings.Split(s, ",")
}

func mapPartitionFilter(b *models.Backup, c *models.Common) ([]*aerospike.PartitionFilter, error) {
	switch {
	case b.AfterDigest != "":
		afterDigestFilter, err := backup.NewPartitionFilterAfterDigest(c.Namespace, b.AfterDigest)
		if err != nil {
			return nil, fmt.Errorf("failed to parse after digest filter: %w", err)
		}

		return []*aerospike.PartitionFilter{afterDigestFilter}, nil
	case b.PartitionList != "":
		filterSlice := splitByComma(b.PartitionList)
		partitionFilters := make([]*aerospike.PartitionFilter, 0, len(filterSlice))

		for i := range filterSlice {
			partitionFilter, err := parsePartitionFilter(c.Namespace, filterSlice[i])
			if err != nil {
				return nil, err
			}

			partitionFilters = append(partitionFilters, partitionFilter)
		}

		return partitionFilters, nil
	default:
		return []*aerospike.PartitionFilter{backup.NewPartitionFilterAll()}, nil
	}
}

// parsePartitionFilter check inputs from --partition-list with regexp.
// Parse values and returns *aerospike.PartitionFilter or error
func parsePartitionFilter(namespace, filter string) (*aerospike.PartitionFilter, error) {
	// Range 0-4096
	if expPartitionRange.MatchString(filter) {
		return parsePartitionFilterByRange(filter)
	}

	// Id 1456
	if expPartitionID.MatchString(filter) {
		return parsePartitionFilterByID(filter)
	}

	// Digest (base64 string)
	if expPartitionDigest.MatchString(filter) {
		return parsePartitionFilterByDigest(namespace, filter)
	}

	return nil, fmt.Errorf("failed to parse partition filter: %s", filter)
}

func parsePartitionFilterByRange(filter string) (*aerospike.PartitionFilter, error) {
	bounds := strings.Split(filter, "-")
	if len(bounds) != 2 {
		return nil, fmt.Errorf("invalid partition filter: %s", filter)
	}

	begin, err := strconv.Atoi(bounds[0])
	if err != nil {
		return nil, fmt.Errorf("invalid partition filter %s begin value: %w", filter, err)
	}

	count, err := strconv.Atoi(bounds[1])
	if err != nil {
		return nil, fmt.Errorf("invalid partition filter %s count value: %w", filter, err)
	}

	return backup.NewPartitionFilterByRange(begin, count), nil
}

func parsePartitionFilterByID(filter string) (*aerospike.PartitionFilter, error) {
	id, err := strconv.Atoi(filter)
	if err != nil {
		return nil, fmt.Errorf("invalid partition filter %s id value: %w", filter, err)
	}

	return backup.NewPartitionFilterByID(id), nil
}

func parsePartitionFilterByDigest(namespace, filter string) (*aerospike.PartitionFilter, error) {
	return backup.NewPartitionFilterByDigest(namespace, filter)
}

func parseLocalTimeToUTC(timeString string) (time.Time, error) {
	location, err := time.LoadLocation("Local")
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to load timezone location: %w", err)
	}

	var validTime string

	switch {
	case expDateTime.MatchString(timeString):
		validTime = timeString
	case expTimeOnly.MatchString(timeString):
		currentTime := time.Now().In(location)
		validTime = currentTime.Format("2006-01-02") + "_" + timeString
	case expDateOnly.MatchString(timeString):
		validTime = timeString + "_00:00:00"
	default:
		return time.Time{}, fmt.Errorf("unknown time format: %s", timeString)
	}

	localTime, err := time.ParseInLocation("2006-01-02_15:04:05", validTime, location)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse time %s: %w", timeString, err)
	}

	utcTime := localTime.UTC()

	return utcTime, nil
}
