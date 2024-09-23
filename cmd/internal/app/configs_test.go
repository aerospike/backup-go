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

func testCompression() *models.Compression {
	return &models.Compression{
		Mode:  "ZSTD",
		Level: 3,
	}
}

func testEncryption() *models.Encryption {
	return &models.Encryption{
		Mode:    "AES256",
		KeyFile: "/path/to/keyfile",
	}
}

func testSecretAgent() *models.SecretAgent {
	return &models.SecretAgent{
		Address:            "localhost",
		ConnectionType:     "tcp",
		Port:               8080,
		TimeoutMillisecond: 1000,
		CaFile:             "/path/to/ca.pem",
		IsBase64:           true,
	}
}

func TestMapBackupConfig_Success(t *testing.T) {
	t.Parallel()
	backupModel := &models.Backup{
		FileLimit:        5000,
		AfterDigest:      "digest",
		ModifiedBefore:   "2023-09-01_12:00:00",
		ModifiedAfter:    "2023-09-02_12:00:00",
		FilterExpression: "k1EDpHRlc3Q=",
		NoTTLOnly:        true,
	}

	commonModel := &models.Common{
		Namespace:        "test-namespace",
		SetList:          []string{"set1", "set2"},
		BinList:          []string{"bin1", "bin2"},
		NoRecords:        true,
		NoIndexes:        false,
		RecordsPerSecond: 1000,
		Nice:             10, // 10 MiB
	}

	compression := testCompression()
	encryption := testEncryption()
	secretAgent := testSecretAgent()

	config, err := mapBackupConfig(backupModel, commonModel, compression, encryption, secretAgent)
	assert.NoError(t, err)
	assert.Equal(t, "test-namespace", config.Namespace)
	assert.ElementsMatch(t, []string{"set1", "set2"}, config.SetList)
	assert.ElementsMatch(t, []string{"bin1", "bin2"}, config.BinList)
	assert.True(t, config.NoRecords)
	assert.Equal(t, 1000, config.RecordsPerSecond)
	assert.Equal(t, int64(5000), config.FileLimit)
	assert.Equal(t, "digest", config.AfterDigest)
	assert.Equal(t, true, config.NoTTLOnly)

	modBefore, _ := time.Parse("2006-01-02_15:04:05", "2023-09-01_12:00:00")
	modAfter, _ := time.Parse("2006-01-02_15:04:05", "2023-09-02_12:00:00")
	assert.Equal(t, &modBefore, config.ModBefore)
	assert.Equal(t, &modAfter, config.ModAfter)

	// Compression, Encryption, and Secret Agent
	assert.NotNil(t, config.CompressionPolicy)
	assert.Equal(t, "ZSTD", config.CompressionPolicy.Mode)
	assert.Equal(t, 3, config.CompressionPolicy.Level)

	assert.NotNil(t, config.EncryptionPolicy)
	assert.Equal(t, "AES256", config.EncryptionPolicy.Mode)
	assert.Equal(t, "/path/to/keyfile", *config.EncryptionPolicy.KeyFile)

	assert.NotNil(t, config.SecretAgentConfig)
	assert.Equal(t, "localhost", *config.SecretAgentConfig.Address)
	assert.Equal(t, "tcp", *config.SecretAgentConfig.ConnectionType)
	assert.Equal(t, 8080, *config.SecretAgentConfig.Port)
}

func TestMapBackupConfig_MissingNamespace(t *testing.T) {
	t.Parallel()
	backupModel := &models.Backup{}
	commonModel := &models.Common{}
	config, err := mapBackupConfig(backupModel, commonModel, nil, nil, nil)
	assert.Error(t, err)
	assert.Nil(t, config)
	assert.Equal(t, "namespace is required", err.Error())
}

func TestMapBackupConfig_InvalidModifiedBefore(t *testing.T) {
	t.Parallel()
	backupModel := &models.Backup{
		ModifiedBefore: "invalid-date",
	}
	commonModel := &models.Common{
		Namespace: "test-namespace",
	}
	config, err := mapBackupConfig(backupModel, commonModel, testCompression(), testEncryption(), testSecretAgent())
	assert.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "failed to parse modified before date")
}

func TestMapBackupConfig_InvalidModifiedAfter(t *testing.T) {
	t.Parallel()
	backupModel := &models.Backup{
		ModifiedAfter: "invalid-date",
	}
	commonModel := &models.Common{
		Namespace: "test-namespace",
	}
	config, err := mapBackupConfig(backupModel, commonModel, testCompression(), testEncryption(), testSecretAgent())
	assert.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "failed to parse modified after date")
}

func TestMapBackupConfig_InvalidExpression(t *testing.T) {
	t.Parallel()
	backupModel := &models.Backup{
		FilterExpression: "invalid-exp",
	}
	commonModel := &models.Common{
		Namespace: "test-namespace",
	}
	config, err := mapBackupConfig(backupModel, commonModel, testCompression(), testEncryption(), testSecretAgent())
	assert.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "failed to parse filter expression")
}

func TestMapRestoreConfig_Success(t *testing.T) {
	t.Parallel()
	restoreModel := &models.Restore{}
	commonModel := &models.Common{
		Namespace:        "test-namespace",
		SetList:          []string{"set1", "set2"},
		BinList:          []string{"bin1", "bin2"},
		NoRecords:        true,
		NoIndexes:        false,
		RecordsPerSecond: 1000,
		Nice:             10, // 10 MiB
	}

	compression := testCompression()
	encryption := testEncryption()
	secretAgent := testSecretAgent()

	config, err := mapRestoreConfig(restoreModel, commonModel, compression, encryption, secretAgent)
	assert.NoError(t, err)
	assert.Equal(t, "test-namespace", *config.Namespace.Source)
	assert.Equal(t, "test-namespace", *config.Namespace.Destination)
	assert.ElementsMatch(t, []string{"set1", "set2"}, config.SetList)
	assert.ElementsMatch(t, []string{"bin1", "bin2"}, config.BinList)
	assert.True(t, config.NoRecords)
	assert.Equal(t, 1000, config.RecordsPerSecond)

	assert.NotNil(t, config.CompressionPolicy)
	assert.Equal(t, "ZSTD", config.CompressionPolicy.Mode)
	assert.Equal(t, 3, config.CompressionPolicy.Level)

	assert.NotNil(t, config.EncryptionPolicy)
	assert.Equal(t, "AES256", config.EncryptionPolicy.Mode)
	assert.Equal(t, "/path/to/keyfile", *config.EncryptionPolicy.KeyFile)

	assert.NotNil(t, config.SecretAgentConfig)
	assert.Equal(t, "localhost", *config.SecretAgentConfig.Address)
	assert.Equal(t, "tcp", *config.SecretAgentConfig.ConnectionType)
	assert.Equal(t, 8080, *config.SecretAgentConfig.Port)
}

func TestMapCompressionPolicy_Success(t *testing.T) {
	t.Parallel()
	compressionModel := testCompression()

	compressionPolicy := mapCompressionPolicy(compressionModel)
	assert.NotNil(t, compressionPolicy)
	assert.Equal(t, "ZSTD", compressionPolicy.Mode)
	assert.Equal(t, 3, compressionPolicy.Level)
}

func TestMapCompressionPolicy_EmptyMode(t *testing.T) {
	t.Parallel()
	compressionModel := &models.Compression{}
	compressionPolicy := mapCompressionPolicy(compressionModel)
	assert.Nil(t, compressionPolicy)
}

func TestMapCompressionPolicy_CaseInsensitiveMode(t *testing.T) {
	t.Parallel()
	compressionModel := &models.Compression{
		Mode:  "zstd", // Lowercase mode
		Level: 3,
	}

	compressionPolicy := mapCompressionPolicy(compressionModel)
	assert.NotNil(t, compressionPolicy)
	assert.Equal(t, "ZSTD", compressionPolicy.Mode, "Compression mode should be converted to uppercase")
	assert.Equal(t, 3, compressionPolicy.Level)
}

// Encryption Tests
func TestMapEncryptionPolicy_Success(t *testing.T) {
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

func TestMapEncryptionPolicy_UpperCaseMode(t *testing.T) {
	t.Parallel()
	encryptionModel := &models.Encryption{
		Mode: "aes256", // Lowercase mode
	}

	encryptionPolicy := mapEncryptionPolicy(encryptionModel)
	assert.NotNil(t, encryptionPolicy)
	assert.Equal(t, "AES256", encryptionPolicy.Mode, "Encryption mode should be converted to uppercase")
}

// Secret Agent Tests
func TestMapSecretAgentConfig_Success(t *testing.T) {
	t.Parallel()
	secretAgentModel := testSecretAgent()

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

func TestMapSecretAgentConfig_PartialConfig(t *testing.T) {
	t.Parallel()
	secretAgentModel := &models.SecretAgent{
		Address: "localhost",
		Port:    8080,
	}

	secretAgentConfig := mapSecretAgentConfig(secretAgentModel)
	assert.NotNil(t, secretAgentConfig)
	assert.Equal(t, "localhost", *secretAgentConfig.Address)
	assert.Equal(t, 8080, *secretAgentConfig.Port)
	assert.Nil(t, secretAgentConfig.CaFile, "CaFile should be nil if not set")
}

func TestMapRestoreNamespace_SuccessSingleNamespace(t *testing.T) {
	t.Parallel()
	ns := "source-ns"
	result := mapRestoreNamespace(ns)
	assert.NotNil(t, result, "Result should not be nil")
	assert.Equal(t, "source-ns", *result.Source, "Source should be 'source-ns'")
	assert.Equal(t, "source-ns", *result.Destination, "Destination should be the same as Source")
}

func TestMapRestoreNamespace_SuccessDifferentNamespaces(t *testing.T) {
	t.Parallel()
	ns := "source-ns,destination-ns"
	result := mapRestoreNamespace(ns)
	assert.NotNil(t, result, "Result should not be nil")
	assert.Equal(t, "source-ns", *result.Source, "Source should be 'source-ns'")
	assert.Equal(t, "destination-ns", *result.Destination, "Destination should be 'destination-ns'")
}

func TestMapRestoreNamespace_InvalidNamespace(t *testing.T) {
	t.Parallel()
	ns := "source-ns,destination-ns,extra-ns"
	result := mapRestoreNamespace(ns)
	assert.Nil(t, result, "Result should be nil for invalid input")
}
