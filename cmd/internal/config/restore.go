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
	"runtime"

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/tools-common-go/client"
)

// RestoreServiceConfig contains configuration settings for the restore service,
// including client, restore, and storage details.
type RestoreServiceConfig struct {
	App          *models.App
	ClientConfig *client.AerospikeConfig
	ClientPolicy *models.ClientPolicy
	Restore      *models.Restore
	Compression  *models.Compression
	Encryption   *models.Encryption
	SecretAgent  *models.SecretAgent
	AwsS3        *models.AwsS3
	GcpStorage   *models.GcpStorage
	AzureBlob    *models.AzureBlob
}

// NewRestoreServiceConfig creates and returns a new RestoreServiceConfig initialized with the provided parameters.
// If a config file path is specified in the app, parameters are loaded from the file instead.
// Returns an error if the config file cannot be loaded or parsed.
func NewRestoreServiceConfig(
	app *models.App,
	clientConfig *client.AerospikeConfig,
	clientPolicy *models.ClientPolicy,
	restore *models.Restore,
	compression *models.Compression,
	encryption *models.Encryption,
	secretAgent *models.SecretAgent,
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
) (*RestoreServiceConfig, error) {
	// If we have a config file, load serviceConfig from it.
	if app.ConfigFilePath != "" {
		serviceConfig, err := decodeRestoreServiceConfig(app.ConfigFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to load config file %s: %w", app.ConfigFilePath, err)
		}

		return serviceConfig, nil
	}

	return &RestoreServiceConfig{
		App:          app,
		ClientConfig: clientConfig,
		ClientPolicy: clientPolicy,
		Restore:      restore,
		Compression:  compression,
		Encryption:   encryption,
		SecretAgent:  secretAgent,
		AwsS3:        awsS3,
		GcpStorage:   gcpStorage,
		AzureBlob:    azureBlob,
	}, nil
}

// NewRestoreConfig creates and returns a new ConfigRestore object, initialized with given restore parameters.
func NewRestoreConfig(serviceConfig *RestoreServiceConfig, logger *slog.Logger) *backup.ConfigRestore {
	logger.Info("initializing restore config")

	parallel := runtime.NumCPU()
	if serviceConfig.Restore.Parallel > 0 {
		parallel = serviceConfig.Restore.Parallel
	}

	c := backup.NewDefaultRestoreConfig()
	c.Namespace = newRestoreNamespace(serviceConfig.Restore.Namespace)
	c.SetList = SplitByComma(serviceConfig.Restore.SetList)
	c.BinList = SplitByComma(serviceConfig.Restore.BinList)
	c.NoRecords = serviceConfig.Restore.NoRecords
	c.NoIndexes = serviceConfig.Restore.NoIndexes
	c.NoUDFs = serviceConfig.Restore.NoUDFs
	c.RecordsPerSecond = serviceConfig.Restore.RecordsPerSecond
	c.Parallel = parallel
	c.WritePolicy = newWritePolicy(serviceConfig.Restore)
	c.InfoPolicy = newInfoPolicy(serviceConfig.Restore.TimeOut)
	// As we set --storage-bandwidth-limit in MiB we must convert it to bytes
	c.Bandwidth = serviceConfig.Restore.BandwidthLimit * 1024 * 1024
	c.ExtraTTL = serviceConfig.Restore.ExtraTTL
	c.IgnoreRecordError = serviceConfig.Restore.IgnoreRecordError
	c.DisableBatchWrites = serviceConfig.Restore.DisableBatchWrites
	c.BatchSize = serviceConfig.Restore.BatchSize
	c.MaxAsyncBatches = serviceConfig.Restore.MaxAsyncBatches
	c.MetricsEnabled = true

	c.CompressionPolicy = newCompressionPolicy(serviceConfig.Compression)
	c.EncryptionPolicy = newEncryptionPolicy(serviceConfig.Encryption)
	c.SecretAgentConfig = newSecretAgentConfig(serviceConfig.SecretAgent)
	c.RetryPolicy = newRetryPolicy(
		serviceConfig.Restore.RetryBaseTimeout,
		serviceConfig.Restore.RetryMultiplier,
		serviceConfig.Restore.RetryMaxRetries,
	)
	c.ValidateOnly = serviceConfig.Restore.ValidateOnly

	if !c.ValidateOnly {
		logRestoreConfig(logger, serviceConfig, c)
	}

	return c
}

func logRestoreConfig(logger *slog.Logger, params *RestoreServiceConfig, restoreConfig *backup.ConfigRestore) {
	encryptionMode := "none"
	if params.Encryption != nil {
		encryptionMode = params.Encryption.Mode
	}

	compressLevel := 0
	if params.Compression != nil {
		compressLevel = params.Compression.Level
	}

	logger.Info("initialized restore config",
		slog.Any("namespace_source", *restoreConfig.Namespace.Source),
		slog.Any("namespace_destination", *restoreConfig.Namespace.Destination),
		slog.String("encryption", encryptionMode),
		slog.Int("compression", compressLevel),
		slog.Any("retry", *restoreConfig.RetryPolicy),
		slog.Any("sets", restoreConfig.SetList),
		slog.Any("bins", restoreConfig.BinList),
		slog.Int("parallel", restoreConfig.Parallel),
		slog.Int64("bandwidth", restoreConfig.Bandwidth),
		slog.Bool("no_records", restoreConfig.NoRecords),
		slog.Bool("no_indexes", restoreConfig.NoIndexes),
		slog.Bool("no_udfs", restoreConfig.NoUDFs),
		slog.Bool("disable_batch_writes", restoreConfig.DisableBatchWrites),
		slog.Int("batch_size", restoreConfig.BatchSize),
		slog.Int("max_asynx_batches", restoreConfig.MaxAsyncBatches),
		slog.Int64("extra_ttl", restoreConfig.ExtraTTL),
		slog.Bool("ignore_records_error", restoreConfig.IgnoreRecordError),
	)
}
