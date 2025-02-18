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

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
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
		AfterDigest:      "AvDsV2KuSZHZugDBftnLxGpR+88=",
		ModifiedBefore:   "2023-09-01_12:00:00",
		ModifiedAfter:    "2023-09-02_12:00:00",
		FilterExpression: "k1EDpHRlc3Q=",
		ParallelNodes:    true,
		Compact:          true,
		NodeList:         "node1,node2",
		NoTTLOnly:        true,
	}

	commonModel := &models.Common{
		Namespace:        "test-namespace",
		SetList:          "set1,set2",
		BinList:          "bin1,bin2",
		NoRecords:        true,
		NoIndexes:        false,
		RecordsPerSecond: 1000,
		Nice:             10,
		Parallel:         5,
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
	assert.False(t, config.NoIndexes)
	assert.Equal(t, 1000, config.RecordsPerSecond)
	assert.Equal(t, int64(5000), config.FileLimit)
	assert.Equal(t, true, config.NoTTLOnly)

	modBefore, err := parseLocalTimeToUTC("2023-09-01_12:00:00")
	assert.NoError(t, err)
	modAfter, err := parseLocalTimeToUTC("2023-09-02_12:00:00")
	assert.NoError(t, err)
	assert.Equal(t, modBefore, *config.ModBefore)
	assert.Equal(t, modAfter, *config.ModAfter)

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

	assert.Equal(t, 5, config.ParallelWrite, "The ParallelWrite should be set correctly")
	assert.Equal(t, 5, config.ParallelRead, "The ParallelRead should be set correctly")
	assert.Equal(t, 10*1024*1024, config.Bandwidth, "The Bandwidth should be set to 10 MiB in bytes")
	assert.True(t, config.ParallelNodes, "The ParallelNodes flag should be set correctly")
	assert.True(t, config.Compact, "The Compact flag should be set correctly")
	assert.ElementsMatch(t, []string{"node1", "node2"}, config.NodeList, "The NodeList should be set correctly")
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
		SetList:          "set1,set2",
		BinList:          "bin1,bin2",
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

func TestMapPartitionFilter_AfterDigest(t *testing.T) {
	t.Parallel()
	backupModel := &models.Backup{
		AfterDigest: "AvDsV2KuSZHZugDBftnLxGpR+88=",
	}

	commonModel := &models.Common{
		Namespace: "test-namespace",
	}

	filters, err := mapPartitionFilter(backupModel, commonModel)
	assert.NoError(t, err)
	assert.NotNil(t, filters)
	assert.Equal(t, 1, len(filters))
	assert.IsType(t, &aerospike.PartitionFilter{}, filters[0])
}

func TestMapPartitionFilter_PartitionList(t *testing.T) {
	t.Parallel()
	backupModel := &models.Backup{
		PartitionList: "0-1024",
	}

	commonModel := &models.Common{
		Namespace: "test-namespace",
	}

	filters, err := mapPartitionFilter(backupModel, commonModel)
	assert.NoError(t, err)
	assert.NotNil(t, filters)
	assert.Equal(t, 1, len(filters))
	assert.IsType(t, &aerospike.PartitionFilter{}, filters[0])
}

func TestMapPartitionFilter_NoFilters(t *testing.T) {
	t.Parallel()
	backupModel := &models.Backup{}

	commonModel := &models.Common{
		Namespace: "test-namespace",
	}

	filters, err := mapPartitionFilter(backupModel, commonModel)
	assert.NoError(t, err)
	assert.NotNil(t, filters)
	assert.Equal(t, 1, len(filters))
	assert.Equal(t, backup.NewPartitionFilterAll(), filters[0])
}

func TestParsePartitionFilterByRange_Valid(t *testing.T) {
	t.Parallel()
	filter := "100-200"
	parsedFilter, err := parsePartitionFilterByRange(filter)
	assert.NoError(t, err)
	assert.NotNil(t, parsedFilter)
}

func TestParsePartitionFilterByRange_InvalidRange(t *testing.T) {
	t.Parallel()
	filter := "invalid-range"
	parsedFilter, err := parsePartitionFilterByRange(filter)
	assert.Error(t, err)
	assert.Nil(t, parsedFilter)
	assert.Contains(t, err.Error(), "invalid partition filter")
}

func TestParsePartitionFilterByID_Valid(t *testing.T) {
	t.Parallel()
	filter := "1234"
	parsedFilter, err := parsePartitionFilterByID(filter)
	assert.NoError(t, err)
	assert.NotNil(t, parsedFilter)
}

func TestParsePartitionFilterByID_InvalidID(t *testing.T) {
	t.Parallel()
	filter := "invalid-id"
	parsedFilter, err := parsePartitionFilterByID(filter)
	assert.Error(t, err)
	assert.Nil(t, parsedFilter)
	assert.Contains(t, err.Error(), "invalid partition filter")
}

func TestParsePartitionFilterByDigest_Valid(t *testing.T) {
	t.Parallel()
	namespace := "test-namespace"
	filter := "EjRWeJq83vEjRRI0VniavN7xI0U=" // Base64-encoded digest
	parsedFilter, err := parsePartitionFilterByDigest(namespace, filter)
	assert.NoError(t, err)
	assert.NotNil(t, parsedFilter)
}

func TestParsePartitionFilterByDigest_InvalidDigest(t *testing.T) {
	t.Parallel()
	namespace := "test-namespace"
	filter := "invalid-digest"
	parsedFilter, err := parsePartitionFilterByDigest(namespace, filter)
	assert.Error(t, err)
	assert.Nil(t, parsedFilter)
	assert.Contains(t, err.Error(), "failed to decode after-digest")
}

func TestParsePartitionFilter_InvalidFilter(t *testing.T) {
	t.Parallel()
	namespace := "test-namespace"
	filter := "invalid-filter"
	parsedFilter, err := parsePartitionFilter(namespace, filter)
	assert.Error(t, err)
	assert.Nil(t, parsedFilter)
	assert.Contains(t, err.Error(), "failed to parse partition filter")
}

func TestMapRestoreConfig_MissingNamespace(t *testing.T) {
	t.Parallel()
	restoreModel := &models.Restore{}
	commonModel := &models.Common{}
	config, err := mapRestoreConfig(restoreModel, commonModel, nil, nil, nil)
	assert.Error(t, err)
	assert.Nil(t, config)
	assert.Equal(t, "namespace is required", err.Error())
}

func TestMapRestoreConfig_PartialConfig(t *testing.T) {
	t.Parallel()
	restoreModel := &models.Restore{
		ExtraTTL:           3600,
		IgnoreRecordError:  true,
		DisableBatchWrites: true,
		BatchSize:          1000,
		MaxAsyncBatches:    5,
	}

	commonModel := &models.Common{
		Namespace: "test-namespace",
	}

	config, err := mapRestoreConfig(restoreModel, commonModel, testCompression(), testEncryption(), testSecretAgent())
	assert.NoError(t, err)
	assert.Equal(t, int64(3600), config.ExtraTTL)
	assert.True(t, config.IgnoreRecordError)
	assert.True(t, config.DisableBatchWrites)
	assert.Equal(t, 1000, config.BatchSize)
	assert.Equal(t, 5, config.MaxAsyncBatches)
}

func TestMapScanPolicy_Success(t *testing.T) {
	t.Parallel()
	backupModel := &models.Backup{
		MaxRecords:          500,
		SleepBetweenRetries: 50,
		FilterExpression:    "k1EDpHRlc3Q=",
		PreferRacks:         "rack1",
		NoBins:              true,
	}

	commonModel := &models.Common{
		MaxRetries:    3,
		TotalTimeout:  10000,
		SocketTimeout: 3000,
	}

	scanPolicy, err := mapScanPolicy(backupModel, commonModel)
	assert.NoError(t, err)
	assert.Equal(t, int64(500), scanPolicy.MaxRecords)
	assert.Equal(t, 3, scanPolicy.MaxRetries)
	assert.Equal(t, 50*time.Millisecond, scanPolicy.SleepBetweenRetries)
	assert.Equal(t, 10000*time.Millisecond, scanPolicy.TotalTimeout)
	assert.Equal(t, 3000*time.Millisecond, scanPolicy.SocketTimeout)
	assert.Equal(t, aerospike.PREFER_RACK, scanPolicy.ReplicaPolicy)
	assert.False(t, scanPolicy.IncludeBinData)
}

func TestMapWritePolicy_Success(t *testing.T) {
	t.Parallel()
	restoreModel := &models.Restore{
		Replace: true,
		Uniq:    false,
	}

	commonModel := &models.Common{
		MaxRetries:    3,
		TotalTimeout:  5000,
		SocketTimeout: 1500,
	}

	writePolicy := mapWritePolicy(restoreModel, commonModel)
	assert.Equal(t, aerospike.REPLACE, writePolicy.RecordExistsAction)
	assert.Equal(t, 3, writePolicy.MaxRetries)
	assert.Equal(t, 5000*time.Millisecond, writePolicy.TotalTimeout)
	assert.Equal(t, 1500*time.Millisecond, writePolicy.SocketTimeout)
}

func TestSplitByComma_EmptyString(t *testing.T) {
	t.Parallel()
	result := splitByComma("")
	assert.Nil(t, result)
}

func TestSplitByComma_NonEmptyString(t *testing.T) {
	t.Parallel()
	result := splitByComma("item1,item2,item3")
	assert.Equal(t, []string{"item1", "item2", "item3"}, result)
}

func TestRecordExistsAction(t *testing.T) {
	t.Parallel()
	assert.Equal(t, aerospike.REPLACE, recordExistsAction(true, false))
	assert.Equal(t, aerospike.CREATE_ONLY, recordExistsAction(false, true))
	assert.Equal(t, aerospike.UPDATE, recordExistsAction(false, false))
}

func TestParseLocalTimeToUTC(t *testing.T) {
	tests := []struct {
		name        string
		timeString  string
		expectedUTC string
		expectError bool
		errorText   string
	}{
		{
			name:        "Valid DateTime",
			timeString:  "2023-09-01_12:34:56",
			expectedUTC: "2023-09-01_12:34:56",
			expectError: false,
		},
		{
			name:        "Valid Date Only",
			timeString:  "2023-09-01",
			expectedUTC: "2023-09-01_00:00:00",
			expectError: false,
		},
		{
			name:        "Valid Time Only",
			timeString:  "12:34:56",
			expectedUTC: time.Now().Format("2006-01-02") + "_12:34:56",
			expectError: false,
		},
		{
			name:        "Invalid Format",
			timeString:  "invalid-format",
			expectedUTC: "",
			expectError: true,
			errorText:   "unknown time format",
		},
		{
			name:        "Invalid Date",
			timeString:  "2023-13-01_12:00:00",
			expectedUTC: "",
			expectError: true,
			errorText:   "failed to parse time",
		},
		{
			name:        "Invalid Time",
			timeString:  "2023-09-01_25:00:00",
			expectedUTC: "",
			expectError: true,
			errorText:   "failed to parse time",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseLocalTimeToUTC(tt.timeString)

			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, time.Time{}, result)
				assert.Contains(t, err.Error(), tt.errorText)
			} else {
				assert.NoError(t, err)
				location, err := time.LoadLocation("Local")
				assert.NoError(t, err)
				localTime, err := time.ParseInLocation("2006-01-02_15:04:05", tt.expectedUTC, location)
				assert.NoError(t, err)
				assert.Equal(t, localTime.UTC(), result)
			}
		})
	}
}
