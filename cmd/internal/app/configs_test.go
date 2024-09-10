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
	"testing"
	"time"

	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/stretchr/testify/assert"
)

func TestMapBackupConfig_Success(t *testing.T) {
	t.Parallel()
	backupModel := &models.Backup{
		Common: models.Common{
			Namespace:        "test-namespace",
			SetList:          []string{"set1", "set2"},
			BinList:          []string{"bin1", "bin2"},
			NoRecords:        true,
			NoIndexes:        false,
			RecordsPerSecond: 1000,
		},
		FileLimit:        5000,
		AfterDigest:      "digest",
		ModifiedBefore:   "2023-09-01_12:00:00",
		ModifiedAfter:    "2023-09-02_12:00:00",
		FilterExpression: "k1EDpHRlc3Q=",
	}

	config, err := mapBackupConfig(backupModel)
	assert.NoError(t, err)
	assert.Equal(t, "test-namespace", config.Namespace)
	assert.ElementsMatch(t, []string{"set1", "set2"}, config.SetList)
	assert.ElementsMatch(t, []string{"bin1", "bin2"}, config.BinList)
	assert.True(t, config.NoRecords)
	assert.Equal(t, 1000, config.RecordsPerSecond)
	assert.Equal(t, int64(5000), config.FileLimit)
	assert.Equal(t, "digest", config.AfterDigest)

	modBefore, _ := time.Parse("2006-01-02_15:04:05", "2023-09-01_12:00:00")
	modAfter, _ := time.Parse("2006-01-02_15:04:05", "2023-09-02_12:00:00")
	assert.Equal(t, &modBefore, config.ModBefore)
	assert.Equal(t, &modAfter, config.ModAfter)
}

func TestMapBackupConfig_MissingNamespace(t *testing.T) {
	t.Parallel()
	backupModel := &models.Backup{}
	config, err := mapBackupConfig(backupModel)
	assert.Error(t, err)
	assert.Nil(t, config)
	assert.Equal(t, "namespace is required", err.Error())
}

func TestMapBackupConfig_InvalidModifiedBefore(t *testing.T) {
	t.Parallel()
	backupModel := &models.Backup{
		Common: models.Common{
			Namespace: "test-namespace",
		},
		ModifiedBefore: "invalid-date",
	}
	config, err := mapBackupConfig(backupModel)
	assert.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "failed to parse modified before date")
}

func TestMapBackupConfig_InvalidModifiedAfter(t *testing.T) {
	t.Parallel()
	backupModel := &models.Backup{
		Common: models.Common{
			Namespace: "test-namespace",
		},
		ModifiedAfter: "invalid-date",
	}
	config, err := mapBackupConfig(backupModel)
	assert.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "failed to parse modified after date")
}

func TestMapBackupConfig_InvalidExpression(t *testing.T) {
	t.Parallel()
	backupModel := &models.Backup{
		Common: models.Common{
			Namespace: "test-namespace",
		},
		FilterExpression: "invalid-exp",
	}
	config, err := mapBackupConfig(backupModel)
	assert.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "failed to parse filter expression")
}

func TestMapCompressionPolicy(t *testing.T) {
	t.Parallel()
	compressionModel := &models.Compression{
		Mode:  "ZSTD",
		Level: 5,
	}

	compressionPolicy := mapCompressionPolicy(compressionModel)
	assert.NotNil(t, compressionPolicy)
	assert.Equal(t, "ZSTD", compressionPolicy.Mode)
	assert.Equal(t, 5, compressionPolicy.Level)
}

func TestMapCompressionPolicy_EmptyMode(t *testing.T) {
	t.Parallel()
	compressionModel := &models.Compression{}
	compressionPolicy := mapCompressionPolicy(compressionModel)
	assert.Nil(t, compressionPolicy)
}

func TestMapEncryptionPolicy(t *testing.T) {
	t.Parallel()
	encryptionModel := &models.Encryption{
		Mode:      "AES256",
		KeyFile:   "/path/to/keyfile",
		KeyEnv:    "ENV_KEY",
		KeySecret: "secret",
	}

	encryptionPolicy := mapEncryptionPolicy(encryptionModel)
	assert.NotNil(t, encryptionPolicy)
	assert.Equal(t, "AES256", encryptionPolicy.Mode)
	assert.Equal(t, "/path/to/keyfile", *encryptionPolicy.KeyFile)
	assert.Equal(t, "ENV_KEY", *encryptionPolicy.KeyEnv)
	assert.Equal(t, "secret", *encryptionPolicy.KeySecret)
}

func TestMapEncryptionPolicy_EmptyMode(t *testing.T) {
	t.Parallel()
	encryptionModel := &models.Encryption{}
	encryptionPolicy := mapEncryptionPolicy(encryptionModel)
	assert.Nil(t, encryptionPolicy)
}

func TestMapSecretAgentConfig(t *testing.T) {
	t.Parallel()
	secretAgentModel := &models.SecretAgent{
		Address:            "localhost",
		ConnectionType:     "tcp",
		Port:               8080,
		TimeoutMillisecond: 1000,
		CaFile:             "/path/to/ca.pem",
		IsBase64:           true,
	}

	secretAgentConfig := mapSecretAgentConfig(secretAgentModel)
	assert.NotNil(t, secretAgentConfig)
	assert.Equal(t, "localhost", *secretAgentConfig.Address)
	assert.Equal(t, "tcp", *secretAgentConfig.ConnectionType)
	assert.Equal(t, 8080, *secretAgentConfig.Port)
	assert.Equal(t, 1000, *secretAgentConfig.TimeoutMillisecond)
	assert.Equal(t, "/path/to/ca.pem", *secretAgentConfig.CaFile)
	assert.True(t, *secretAgentConfig.IsBase64)
}

func TestMapSecretAgentConfig_EmptyAddress(t *testing.T) {
	t.Parallel()
	secretAgentModel := &models.SecretAgent{}
	secretAgentConfig := mapSecretAgentConfig(secretAgentModel)
	assert.Nil(t, secretAgentConfig)
}

func TestMapScanPolicy_Success(t *testing.T) {
	t.Parallel()
	backupModel := &models.Backup{
		MaxRecords:          1000,
		SleepBetweenRetries: 100,
		NoBins:              true,
		FilterExpression:    "k1EDpHRlc3Q=",
		Common: models.Common{
			MaxRetries:    3,
			TotalTimeout:  5000,
			SocketTimeout: 3000,
		},
	}

	scanPolicy, err := mapScanPolicy(backupModel)
	assert.NoError(t, err)
	assert.Equal(t, int64(1000), scanPolicy.MaxRecords)
	assert.Equal(t, 3, scanPolicy.MaxRetries)
	assert.Equal(t, 100*time.Millisecond, scanPolicy.SleepBetweenRetries)
	assert.Equal(t, 5000*time.Millisecond, scanPolicy.TotalTimeout)
	assert.Equal(t, 3000*time.Millisecond, scanPolicy.SocketTimeout)
	assert.False(t, scanPolicy.IncludeBinData)
}

func TestMapScanPolicy_FailedFilterExpression(t *testing.T) {
	t.Parallel()
	backupModel := &models.Backup{
		FilterExpression: "invalid-base64",
	}

	scanPolicy, err := mapScanPolicy(backupModel)
	assert.Error(t, err)
	assert.Nil(t, scanPolicy)
	assert.Contains(t, err.Error(), "failed to parse filter expression")
}
