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
	// Time parsing expressions.
	expTimeOnly = regexp.MustCompile(`^\d{2}:\d{2}:\d{2}$`)
	expDateOnly = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	expDateTime = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}$`)
)

// NewSecretAgent determines and returns the SecretAgentConfig from ConfigBackup or ConfigBackupXDR.
// Returns nil if both are nil.
func NewSecretAgent(b *backup.ConfigBackup, bxdr *backup.ConfigBackupXDR) *backup.SecretAgentConfig {
	switch {
	case b != nil:
		return b.SecretAgentConfig
	case bxdr != nil:
		return bxdr.SecretAgentConfig
	default:
		return nil
	}
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
			return nil, fmt.Errorf("rack id %d invalid, should be non-negative number", rackID)
		}

		if rackID > MaxRack {
			return nil, fmt.Errorf("rack id %d invalid, should not exceed %d", rackID, MaxRack)
		}

		racksIntSlice = append(racksIntSlice, rackID)
	}

	return racksIntSlice, nil
}

// newRestoreNamespace creates and returns a RestoreNamespaceConfig with source and destination namespaces
// derived from input. Accepts a comma-separated string. If one namespace is provided,
// it sets both source and destination to the same value.
// Returns nil if invalid input (e.g., more than two namespaces) is provided.
func newRestoreNamespace(n string) *backup.RestoreNamespaceConfig {
	nsArr := SplitByComma(n)

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

// newCompressionPolicy maps a Compression model to a CompressionPolicy. Returns nil if the input is nil or invalid.
func newCompressionPolicy(c *models.Compression) *backup.CompressionPolicy {
	if c == nil {
		return nil
	}

	if c.Mode == "" {
		return nil
	}

	return backup.NewCompressionPolicy(strings.ToUpper(c.Mode), c.Level)
}

func newEncryptionPolicy(e *models.Encryption) *backup.EncryptionPolicy {
	if e == nil {
		return nil
	}

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

func newSecretAgentConfig(s *models.SecretAgent) *backup.SecretAgentConfig {
	if s == nil {
		return nil
	}

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

func newScanPolicy(b *models.Backup) (*aerospike.ScanPolicy, error) {
	p := aerospike.NewScanPolicy()
	p.MaxRecords = b.MaxRecords
	p.MaxRetries = b.MaxRetries
	p.SleepBetweenRetries = time.Duration(b.SleepBetweenRetries) * time.Millisecond
	p.TotalTimeout = time.Duration(b.TotalTimeout) * time.Millisecond
	p.SocketTimeout = time.Duration(b.SocketTimeout) * time.Millisecond
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

func newWritePolicy(r *models.Restore) *aerospike.WritePolicy {
	p := aerospike.NewWritePolicy(0, 0)

	p.SendKey = true
	p.MaxRetries = r.MaxRetries
	p.TotalTimeout = time.Duration(r.TotalTimeout) * time.Millisecond
	p.SocketTimeout = time.Duration(r.SocketTimeout) * time.Millisecond
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

func newInfoPolicy(timeOut int64) *aerospike.InfoPolicy {
	p := aerospike.NewInfoPolicy()
	p.Timeout = time.Duration(timeOut) * time.Millisecond

	return p
}

func newRetryPolicy(retryBaseTimeout int64, retryMultiplier float64, retryMaxRetries uint) *bModels.RetryPolicy {
	return bModels.NewRetryPolicy(
		time.Duration(retryBaseTimeout)*time.Millisecond,
		retryMultiplier,
		retryMaxRetries,
	)
}

// SplitByComma splits a comma-separated string into a slice of strings. Returns nil if the input string is empty.
func SplitByComma(s string) []string {
	if s == "" {
		return nil
	}

	return strings.Split(s, ",")
}

func mapPartitionFilter(b *models.Backup) ([]*aerospike.PartitionFilter, error) {
	switch {
	case b.AfterDigest != "":
		afterDigestFilter, err := backup.NewPartitionFilterAfterDigest(b.Namespace, b.AfterDigest)
		if err != nil {
			return nil, fmt.Errorf("failed to parse after digest filter: %w", err)
		}

		return []*aerospike.PartitionFilter{afterDigestFilter}, nil
	case b.PartitionList != "":
		return backup.ParsePartitionFilterListString(b.Namespace, b.PartitionList)
	default:
		return []*aerospike.PartitionFilter{backup.NewPartitionFilterAll()}, nil
	}
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
