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

type RestoreParams struct {
	App          *models.App             `yaml:"app,omitempty"`
	ClientConfig *client.AerospikeConfig `yaml:"-,omitempty"`
	// ClientAerospike is  wrapper for aerospike client params, to unmarshal YAML.
	// Because ClientConfig can't be used because of TLS configuration.
	ClientAerospike *models.ClientAerospike `yaml:"cluster,omitempty"`
	ClientPolicy    *models.ClientPolicy    `yaml:"-,omitempty"`
	Restore         *models.Restore         `yaml:"restore,omitempty"`
	Compression     *models.Compression     `yaml:"compression,omitempty"`
	Encryption      *models.Encryption      `yaml:"encryption,omitempty"`
	SecretAgent     *models.SecretAgent     `yaml:"secret-agent,omitempty"`
	AwsS3           *models.AwsS3           `yaml:"aws,omitempty"`
	GcpStorage      *models.GcpStorage      `yaml:"gcp,omitempty"`
	AzureBlob       *models.AzureBlob       `yaml:"azure,omitempty"`
}

func NewRestoreParams(
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
) (*RestoreParams, error) {
	// If we have a config file, load params from it.
	if app.Config != "" {
		var (
			params RestoreParams
			err    error
		)

		if err = decodeFromFile(app.Config, &params); err != nil {
			return nil, fmt.Errorf("failed to load config file %s: %w", app.Config, err)
		}
		// Remap config back to ClientConfig.
		params.ClientConfig, err = params.ClientAerospike.ToConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to remap config file from yaml %s: %w", app.Config, err)
		}

		params.ClientPolicy = params.ClientAerospike.ToClientPolicy()

		return &params, nil
	}

	return &RestoreParams{
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
func NewRestoreConfig(params *RestoreParams, logger *slog.Logger) *backup.ConfigRestore {
	logger.Info("initializing restore config")

	parallel := runtime.NumCPU()
	if params.Restore.Parallel > 0 {
		parallel = params.Restore.Parallel
	}

	c := backup.NewDefaultRestoreConfig()
	c.Namespace = newRestoreNamespace(params.Restore.Namespace)
	c.SetList = SplitByComma(params.Restore.SetList)
	c.BinList = SplitByComma(params.Restore.BinList)
	c.NoRecords = params.Restore.NoRecords
	c.NoIndexes = params.Restore.NoIndexes
	c.NoUDFs = params.Restore.NoUDFs
	c.RecordsPerSecond = params.Restore.RecordsPerSecond
	c.Parallel = parallel
	c.WritePolicy = newWritePolicy(params.Restore)
	c.InfoPolicy = mapInfoPolicy(params.Restore.TimeOut)
	// As we set --nice in MiB we must convert it to bytes
	c.Bandwidth = params.Restore.Nice * 1024 * 1024
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
	c.ValidateOnly = params.Restore.ValidateOnly

	if !c.ValidateOnly {
		logRestoreConfig(logger, params, c)
	}

	return c
}

func logRestoreConfig(logger *slog.Logger, params *RestoreParams, restoreConfig *backup.ConfigRestore) {
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
		slog.Int("bandwidth", restoreConfig.Bandwidth),
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
