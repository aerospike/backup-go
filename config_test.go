// Copyright 2024-2026 Aerospike, Inc.
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

package backup

import (
	"testing"
	"time"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testSetName        = "set1"
	testBinName        = "bin1"
	testNodeName       = "node1"
	testNamespaceName  = "ns1"
	testConnectionType = "tcp"
	testKeyFile        = "keyFile"
	testKeyEnv         = "keyEnv"
	testInvalidMode    = "NA"
)

func TestBackupConfig_validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(c *ConfigBackup)
		wantErr string
	}{
		{
			name:    "negative parallel read",
			setup:   func(c *ConfigBackup) { c.ParallelRead = -1 },
			wantErr: "parallel read",
		},
		{
			name:    "negative parallel write",
			setup:   func(c *ConfigBackup) { c.ParallelWrite = -1 },
			wantErr: "parallel write",
		},
		{
			name: "modified before is not after modified after",
			setup: func(c *ConfigBackup) {
				before := time.Now()
				after := before.Add(time.Minute)
				c.ModBefore = &before
				c.ModAfter = &after
			},
			wantErr: "modified before",
		},
		{
			name: "node list combined with partition filter",
			setup: func(c *ConfigBackup) {
				c.NodeList = []string{testNodeName}
				c.PartitionFilters = []*a.PartitionFilter{NewPartitionFilterByID(1)}
			},
			wantErr: "node list or rack list cannot be combined with partition filters or after digest",
		},
		{
			name: "rack list combined with partition filter",
			setup: func(c *ConfigBackup) {
				c.RackList = []int{1}
				c.PartitionFilters = []*a.PartitionFilter{NewPartitionFilterByRange(0, 10)}
			},
			wantErr: "node list or rack list cannot be combined with partition filters or after digest",
		},
		{
			name: "paginated backup without partition filters",
			setup: func(c *ConfigBackup) {
				c.PageSize = 100
				c.PartitionFilters = nil
			},
			wantErr: "partition filters must be set for paginated backup",
		},
		{
			name:    "negative rps",
			setup:   func(c *ConfigBackup) { c.RecordsPerSecond = -1 },
			wantErr: "rps",
		},
		{
			name:    "negative bandwidth",
			setup:   func(c *ConfigBackup) { c.Bandwidth = -1 },
			wantErr: "bandwidth",
		},
		{
			name:    "state file without page size",
			setup:   func(c *ConfigBackup) { c.StateFile = "state.json" },
			wantErr: "page size must be set",
		},
		{
			name:    "continue without state file",
			setup:   func(c *ConfigBackup) { c.Continue = true },
			wantErr: "state file must be set",
		},
		{
			name:    "invalid compression policy",
			setup:   func(c *ConfigBackup) { c.CompressionPolicy = NewCompressionPolicy(CompressZSTD, -2) },
			wantErr: "compression",
		},
		{
			name:    "invalid encryption policy",
			setup:   func(c *ConfigBackup) { c.EncryptionPolicy = &EncryptionPolicy{} },
			wantErr: "encryption",
		},
		{
			name: "invalid secret agent config",
			setup: func(c *ConfigBackup) {
				connectionType := testConnectionType
				c.SecretAgentConfig = &SecretAgentConfig{ConnectionType: &connectionType}
			},
			wantErr: "secret agent",
		},
		{
			name:    "mrt monitor set",
			setup:   func(c *ConfigBackup) { c.SetList = append(c.SetList, models.MonitorRecordsSetName) },
			wantErr: "mrt monitor set is not allowed",
		},
		{
			name:    "duplicated racks",
			setup:   func(c *ConfigBackup) { c.RackList = []int{1, 1} },
			wantErr: "rack list contains duplicates",
		},
		{
			name:    "duplicated nodes",
			setup:   func(c *ConfigBackup) { c.NodeList = []string{testNodeName, testNodeName} },
			wantErr: "node list contains duplicates",
		},
		{
			name:    "duplicated sets",
			setup:   func(c *ConfigBackup) { c.SetList = []string{testSetName, testSetName} },
			wantErr: "set list contains duplicates",
		},
		{
			name:    "duplicated bins",
			setup:   func(c *ConfigBackup) { c.BinList = []string{testBinName, testBinName} },
			wantErr: "bin list contains duplicates",
		},
		{
			name:    "reserved output file prefix",
			setup:   func(c *ConfigBackup) { c.OutputFilePrefix = metadataFileNamePrefix },
			wantErr: "prefix is reserved for metadata files",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := NewDefaultBackupConfig()
			tt.setup(config)

			err := config.validate()
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestBackupConfig_validateDefault(t *testing.T) {
	t.Parallel()

	config := NewDefaultBackupConfig()

	require.NoError(t, config.validate())
	assert.True(t, config.withoutFilter())
}

func TestRestoreConfig_validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(c *ConfigRestore)
		wantErr string
	}{
		{
			name:    "negative parallel",
			setup:   func(c *ConfigRestore) { c.Parallel = -1 },
			wantErr: "parallel",
		},
		{
			name:    "empty namespace",
			setup:   func(c *ConfigRestore) { c.Namespace = &RestoreNamespaceConfig{} },
			wantErr: "source namespace",
		},
		{
			name:    "negative bandwidth",
			setup:   func(c *ConfigRestore) { c.Bandwidth = -1 },
			wantErr: "bandwidth",
		},
		{
			name:    "negative rps",
			setup:   func(c *ConfigRestore) { c.RecordsPerSecond = -1 },
			wantErr: "rps",
		},
		{
			name:    "negative batch size",
			setup:   func(c *ConfigRestore) { c.BatchSize = -1 },
			wantErr: "batch size",
		},
		{
			name:    "negative max async batches",
			setup:   func(c *ConfigRestore) { c.MaxAsyncBatches = -1 },
			wantErr: "async batches",
		},
		{
			name:    "negative extra ttl",
			setup:   func(c *ConfigRestore) { c.ExtraTTL = -1 },
			wantErr: "extra ttl",
		},
		{
			name:    "invalid compression policy",
			setup:   func(c *ConfigRestore) { c.CompressionPolicy = NewCompressionPolicy(CompressZSTD, -2) },
			wantErr: "compression",
		},
		{
			name:    "invalid encryption policy",
			setup:   func(c *ConfigRestore) { c.EncryptionPolicy = &EncryptionPolicy{} },
			wantErr: "encryption",
		},
		{
			name: "invalid secret agent config",
			setup: func(c *ConfigRestore) {
				connectionType := testConnectionType
				c.SecretAgentConfig = &SecretAgentConfig{ConnectionType: &connectionType}
			},
			wantErr: "secret agent",
		},
		{
			name:    "duplicated sets",
			setup:   func(c *ConfigRestore) { c.SetList = []string{testSetName, testSetName} },
			wantErr: "set list contains duplicates",
		},
		{
			name:    "duplicated bins",
			setup:   func(c *ConfigRestore) { c.BinList = []string{testBinName, testBinName} },
			wantErr: "bin list contains duplicates",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := NewDefaultRestoreConfig()
			tt.setup(config)

			err := config.validate()
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestRestoreConfig_validateDefault(t *testing.T) {
	t.Parallel()

	config := NewDefaultRestoreConfig()

	require.NoError(t, config.validate())
}

func TestRestoreNamespaceConfig_validate(t *testing.T) {
	t.Parallel()

	source := testNamespaceName

	tests := []struct {
		name    string
		config  RestoreNamespaceConfig
		wantErr string
	}{
		{
			name:    "no source",
			config:  RestoreNamespaceConfig{},
			wantErr: "source namespace",
		},
		{
			name:    "no destination",
			config:  RestoreNamespaceConfig{Source: &source},
			wantErr: "destination namespace",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.config.validate()
			require.ErrorContains(t, err, tt.wantErr)
			// Nested policies stay unwrapped: ErrInvalidConfig is added once,
			// by the top level ConfigBackup/ConfigRestore validate.
			require.NotErrorIs(t, err, ErrInvalidConfig)
		})
	}
}

func TestCompressionPolicy_validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		policy  CompressionPolicy
		wantErr string
	}{
		{
			name:    "invalid mode",
			policy:  CompressionPolicy{Mode: testInvalidMode},
			wantErr: "invalid compression mode",
		},
		{
			name:    "invalid level",
			policy:  CompressionPolicy{Mode: CompressNone, Level: -2},
			wantErr: "invalid compression level",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.policy.validate()
			require.ErrorContains(t, err, tt.wantErr)
			require.NotErrorIs(t, err, ErrInvalidConfig)
		})
	}
}

func TestEncryptionPolicy_validate(t *testing.T) {
	t.Parallel()

	keyFile := testKeyFile
	keyEnv := testKeyEnv

	tests := []struct {
		name    string
		policy  EncryptionPolicy
		wantErr string
	}{
		{
			name:    "invalid mode",
			policy:  EncryptionPolicy{Mode: testInvalidMode},
			wantErr: "invalid encryption mode",
		},
		{
			name:    "no key location",
			policy:  EncryptionPolicy{Mode: EncryptAES128},
			wantErr: "encryption key location not specified",
		},
		{
			name:    "several key sources",
			policy:  EncryptionPolicy{Mode: EncryptAES128, KeyFile: &keyFile, KeyEnv: &keyEnv},
			wantErr: "only one encryption key source may be specified",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.policy.validate()
			require.ErrorContains(t, err, tt.wantErr)
			require.NotErrorIs(t, err, ErrInvalidConfig)
		})
	}
}

func TestEncryptionPolicy_validateNone(t *testing.T) {
	t.Parallel()

	policy := EncryptionPolicy{Mode: EncryptNone}

	require.NoError(t, policy.validate())
}
