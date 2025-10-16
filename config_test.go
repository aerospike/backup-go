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

package backup

import (
	"strings"
	"testing"
	"time"

	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
)

func TestBackupConfig_validate(t *testing.T) {
	config := NewDefaultBackupConfig()
	assert.NoError(t, config.validate())
	assert.Equal(t, true, config.withoutFilter())

	config.ParallelRead = -1
	assert.ErrorContains(t, config.validate(), "parallel")
	config = NewDefaultBackupConfig()

	config.ParallelWrite = -1
	assert.ErrorContains(t, config.validate(), "parallel")
	config = NewDefaultBackupConfig()

	before := time.Now()
	after := before.Add(time.Minute)
	config.ModBefore = &before
	config.ModAfter = &after
	assert.ErrorContains(t, config.validate(), "modified before")
	config = NewDefaultBackupConfig()

	config.RecordsPerSecond = -1
	assert.ErrorContains(t, config.validate(), "rps")
	config = NewDefaultBackupConfig()

	config.Bandwidth = -1
	assert.ErrorContains(t, config.validate(), "bandwidth")
	config = NewDefaultBackupConfig()

	config.CompressionPolicy = NewCompressionPolicy(CompressZSTD, -2)
	assert.ErrorContains(t, config.validate(), "compression")
	config = NewDefaultBackupConfig()

	config.EncryptionPolicy = &EncryptionPolicy{}
	assert.ErrorContains(t, config.validate(), "encryption")
	config = NewDefaultBackupConfig()

	connectionType := "tcp"
	config.SecretAgentConfig = &SecretAgentConfig{ConnectionType: &connectionType}
	assert.ErrorContains(t, config.validate(), "secret agent")
	config = NewDefaultBackupConfig()

	config.SetList = append(config.SetList, models.MonitorRecordsSetName)
	assert.ErrorContains(t, config.validate(), "mrt monitor set is not allowed")
	config = NewDefaultBackupConfig()

	config.EncoderType = EncoderTypeASBX
	assert.ErrorContains(t, config.validate(), "encoder type")
}

func TestRestoreConfig_validate(t *testing.T) {
	config := NewDefaultRestoreConfig()

	config.Parallel = -1
	assert.ErrorContains(t, config.validate(), "parallel")
	config = NewDefaultRestoreConfig()

	config.Namespace = &RestoreNamespaceConfig{}
	assert.ErrorContains(t, config.validate(), "source namespace")
	config = NewDefaultRestoreConfig()

	config.Bandwidth = -1
	assert.ErrorContains(t, config.validate(), "bandwidth")
	config = NewDefaultRestoreConfig()

	config.RecordsPerSecond = -1
	assert.ErrorContains(t, config.validate(), "rps")
	config = NewDefaultRestoreConfig()

	config.BatchSize = -1
	assert.ErrorContains(t, config.validate(), "batch size")
	config = NewDefaultRestoreConfig()

	config.MaxAsyncBatches = -1
	assert.ErrorContains(t, config.validate(), "async batches")
	config = NewDefaultRestoreConfig()

	config.CompressionPolicy = NewCompressionPolicy(CompressZSTD, -2)
	assert.ErrorContains(t, config.validate(), "compression")
	config = NewDefaultRestoreConfig()

	config.EncryptionPolicy = &EncryptionPolicy{}
	assert.ErrorContains(t, config.validate(), "encryption")
	config = NewDefaultRestoreConfig()

	connectionType := "tcp"
	config.SecretAgentConfig = &SecretAgentConfig{ConnectionType: &connectionType}
	assert.ErrorContains(t, config.validate(), "secret agent")
}

func TestRestoreNamespaceConfig_Validate(t *testing.T) {
	var config RestoreNamespaceConfig
	assert.ErrorContains(t, config.validate(), "source namespace")
	source := "ns1"
	config.Source = &source
	assert.ErrorContains(t, config.validate(), "destination namespace")
}

func TestCompressionPolicy_Validate(t *testing.T) {
	var policy CompressionPolicy
	policy.Mode = "NA"
	assert.ErrorContains(t, policy.validate(), "invalid compression mode")
	policy.Mode = CompressNone
	policy.Level = -2
	assert.ErrorContains(t, policy.validate(), "invalid compression level")
}

func TestEncryptionPolicy_Validate(t *testing.T) {
	var policy EncryptionPolicy
	policy.Mode = "NA"
	assert.ErrorContains(t, policy.validate(), "invalid encryption mode")
	policy.Mode = CompressNone
	assert.NoError(t, policy.validate())
	keyFile := "keyFile"
	policy.KeyFile = &keyFile
	keyEnv := "keyEnv"
	policy.KeyEnv = &keyEnv
	assert.ErrorContains(t, policy.validate(), "only one encryption key source may be specified")
}

func TestConfigRestore_IsValidForASBX(t *testing.T) {
	source := "source-ns"
	destination := "dest-ns"
	sameNs := "same-ns"

	tests := []struct {
		name    string
		config  *ConfigRestore
		wantErr string
	}{
		{
			name: "valid config with same namespace",
			config: &ConfigRestore{
				Namespace: &RestoreNamespaceConfig{
					Source:      &sameNs,
					Destination: &sameNs,
				},
			},
			wantErr: "",
		},
		{
			name: "different source and destination namespaces",
			config: &ConfigRestore{
				Namespace: &RestoreNamespaceConfig{
					Source:      &source,
					Destination: &destination,
				},
			},
			wantErr: "changing namespace is not supported for ASBX",
		},
		{
			name: "set list not empty",
			config: &ConfigRestore{
				Namespace: &RestoreNamespaceConfig{
					Source:      &sameNs,
					Destination: &sameNs,
				},
				SetList: []string{"set1"},
			},
			wantErr: "set list is not supported for ASBX",
		},
		{
			name: "bin list not empty",
			config: &ConfigRestore{
				Namespace: &RestoreNamespaceConfig{
					Source:      &sameNs,
					Destination: &sameNs,
				},
				BinList: []string{"bin1"},
			},
			wantErr: "bin list is not supported for ASBX",
		},
		{
			name: "no records set to true",
			config: &ConfigRestore{
				Namespace: &RestoreNamespaceConfig{
					Source:      &sameNs,
					Destination: &sameNs,
				},
				NoRecords: true,
			},
			wantErr: "no records is not supported for ASBX",
		},
		{
			name: "no indexes set to true",
			config: &ConfigRestore{
				Namespace: &RestoreNamespaceConfig{
					Source:      &sameNs,
					Destination: &sameNs,
				},
				NoIndexes: true,
			},
			wantErr: "no indexes is not supported for ASBX",
		},
		{
			name: "no UDFs set to true",
			config: &ConfigRestore{
				Namespace: &RestoreNamespaceConfig{
					Source:      &sameNs,
					Destination: &sameNs,
				},
				NoUDFs: true,
			},
			wantErr: "no udfs is not supported for ASBX",
		},
		{
			name: "disable batch writes set to true",
			config: &ConfigRestore{
				Namespace: &RestoreNamespaceConfig{
					Source:      &sameNs,
					Destination: &sameNs,
				},
				DisableBatchWrites: true,
			},
			wantErr: "disable batch writes is not supported for ASBX",
		},
		{
			name: "extra TTL greater than zero",
			config: &ConfigRestore{
				Namespace: &RestoreNamespaceConfig{
					Source:      &sameNs,
					Destination: &sameNs,
				},
				ExtraTTL: 1,
			},
			wantErr: "extra ttl value is not supported for ASBX",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.isValidForASBX()
			if tt.wantErr != "" {
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), tt.wantErr),
					"expected error containing '%s', got '%s'", tt.wantErr, err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
