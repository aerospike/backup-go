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
	"runtime"

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/tools-common-go/client"
)

type RestoreParams struct {
	App          *models.App
	ClientConfig *client.AerospikeConfig
	ClientPolicy *models.ClientPolicy
	Restore      *models.Restore
	Common       *models.Common
	Compression  *models.Compression
	Encryption   *models.Encryption
	SecretAgent  *models.SecretAgent
	AwsS3        *models.AwsS3
	GcpStorage   *models.GcpStorage
	AzureBlob    *models.AzureBlob
}

// NewRestoreConfig creates and returns a new ConfigRestore object, initialized with given restore parameters.
func NewRestoreConfig(params *RestoreParams) *backup.ConfigRestore {
	parallel := runtime.NumCPU()
	if params.Common.Parallel > 0 {
		parallel = params.Common.Parallel
	}

	c := backup.NewDefaultRestoreConfig()
	c.Namespace = newRestoreNamespace(params.Common.Namespace)
	c.SetList = SplitByComma(params.Common.SetList)
	c.BinList = SplitByComma(params.Common.BinList)
	c.NoRecords = params.Common.NoRecords
	c.NoIndexes = params.Common.NoIndexes
	c.NoUDFs = params.Common.NoUDFs
	c.RecordsPerSecond = params.Common.RecordsPerSecond
	c.Parallel = parallel
	c.WritePolicy = newWritePolicy(params.Restore, params.Common)
	c.InfoPolicy = mapInfoPolicy(params.Restore.TimeOut)
	// As we set --nice in MiB we must convert it to bytes
	c.Bandwidth = params.Common.Nice * 1024 * 1024
	c.ExtraTTL = params.Restore.ExtraTTL
	c.IgnoreRecordError = params.Restore.IgnoreRecordError
	c.DisableBatchWrites = params.Restore.DisableBatchWrites
	c.BatchSize = params.Restore.BatchSize
	c.MaxAsyncBatches = params.Restore.MaxAsyncBatches
	c.MetricsEnabled = true

	c.CompressionPolicy = newCompressionPolicy(params.Compression)
	c.EncryptionPolicy = newEncryptionPolicy(params.Encryption)
	c.SecretAgentConfig = newSecretAgentConfig(params.SecretAgent)
	c.RetryPolicy = mapRetryPolicy(
		params.Restore.RetryBaseTimeout,
		params.Restore.RetryMultiplier,
		params.Restore.RetryMaxRetries,
	)

	return c
}
