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
	"testing"
	"time"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/stretchr/testify/assert"
)

func TestBackupConfig_validate(t *testing.T) {
	config := NewDefaultBackupConfig()
	assert.NoError(t, config.validate())
	assert.Equal(t, true, config.isFullBackup())

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

	config.ParallelNodes = true
	config.PartitionFilters = []*a.PartitionFilter{NewPartitionFilterByID(1)}
	assert.ErrorContains(t, config.validate(), "parallel by nodes and partitions")
	config = NewDefaultBackupConfig()

	config.Bandwidth = -1
	assert.ErrorContains(t, config.validate(), "bandwidth")
	config = NewDefaultBackupConfig()

	config.FileLimit = -1
	assert.ErrorContains(t, config.validate(), "filelimit")
	config = NewDefaultBackupConfig()

	config.CompressionPolicy = &CompressionPolicy{Level: -1}
	assert.ErrorContains(t, config.validate(), "compression")
	config = NewDefaultBackupConfig()

	config.EncryptionPolicy = &EncryptionPolicy{}
	assert.ErrorContains(t, config.validate(), "encryption")
	config = NewDefaultBackupConfig()

	connectionType := "tcp"
	config.SecretAgentConfig = &SecretAgentConfig{ConnectionType: &connectionType}
	assert.ErrorContains(t, config.validate(), "secret agent")
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

	config.CompressionPolicy = &CompressionPolicy{Level: -1}
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
	assert.ErrorContains(t, policy.validate(), "encryption key location not specified")
	keyFile := "keyFile"
	policy.KeyFile = &keyFile
	keyEnv := "keyEnv"
	policy.KeyEnv = &keyEnv
	assert.ErrorContains(t, policy.validate(), "only one encryption key source may be specified")
}
