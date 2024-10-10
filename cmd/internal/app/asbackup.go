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
	"context"
	"fmt"
	"log/slog"

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/tools-common-go/client"
)

const idBackup = "asbackup-cli"

type ASBackup struct {
	backupClient *backup.Client
	backupConfig *backup.BackupConfig
	writer       backup.Writer
	// reader is used to read state file.
	reader backup.StreamingReader
	// Additional params.
	isEstimate       bool
	estimatesSamples int64
}

func NewASBackup(
	ctx context.Context,
	clientConfig *client.AerospikeConfig,
	backupParams *models.Backup,
	commonParams *models.Common,
	compression *models.Compression,
	encryption *models.Encryption,
	secretAgent *models.SecretAgent,
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
	logger *slog.Logger,
) (*ASBackup, error) {
	// Validations.
	if err := validateStorages(awsS3, gcpStorage, azureBlob); err != nil {
		return nil, err
	}

	if err := validateBackupParams(backupParams, commonParams); err != nil {
		return nil, err
	}

	// Initializations.
	var (
		writer backup.Writer
		reader backup.StreamingReader
		err    error
	)
	// We initialize a writer only if output is configured.
	if backupParams.OutputFile != "" || commonParams.Directory != "" {
		writer, err = getWriter(ctx, backupParams, commonParams, awsS3, gcpStorage, azureBlob)
		if err != nil {
			return nil, fmt.Errorf("failed to create backup writer: %w", err)
		}
		// If asbackup was launched with --remove-artifacts, we don't need to initialize all clients.
		// We clean the folder on writer initialization and exit.
		if backupParams.RemoveArtifacts {
			return nil, nil
		}
	}

	if backupParams.StateFileDst != "" || backupParams.Continue != "" {
		r := &models.Restore{InputFile: backupParams.OutputFile}

		reader, err = getReader(ctx, r, commonParams, awsS3, gcpStorage, azureBlob)
		if err != nil {
			return nil, fmt.Errorf("failed to create reader: %w", err)
		}
	}

	aerospikeClient, err := newAerospikeClient(clientConfig, backupParams.PreferRacks)
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike client: %w", err)
	}

	backupConfig, err := mapBackupConfig(
		backupParams,
		commonParams,
		compression,
		encryption,
		secretAgent,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup config: %w", err)
	}

	backupClient, err := backup.NewClient(aerospikeClient, backup.WithLogger(logger), backup.WithID(idBackup))
	if err != nil {
		return nil, fmt.Errorf("failed to create backup client: %w", err)
	}

	return &ASBackup{
		backupClient:     backupClient,
		backupConfig:     backupConfig,
		writer:           writer,
		reader:           reader,
		isEstimate:       backupParams.Estimate,
		estimatesSamples: backupParams.EstimateSamples,
	}, nil
}

func (b *ASBackup) Run(ctx context.Context) error {
	// If asbackup was called with --remove-artifacts, it would be nil.
	if b == nil {
		return nil
	}

	switch {
	case b.isEstimate:
		estimates, err := b.backupClient.Estimate(ctx, b.backupConfig, b.estimatesSamples)
		if err != nil {
			return fmt.Errorf("failed to calculate backup estimate: %w", err)
		}

		printEstimateReport(estimates)
	default:
		h, err := b.backupClient.Backup(ctx, b.backupConfig, b.writer, b.reader)
		if err != nil {
			return fmt.Errorf("failed to start backup: %w", err)
		}

		if err := h.Wait(ctx); err != nil {
			return fmt.Errorf("failed to backup: %w", err)
		}

		printBackupReport(h.GetStats())
	}

	return nil
}

func getWriter(
	ctx context.Context,
	backupParams *models.Backup,
	commonParams *models.Common,
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
) (backup.Writer, error) {
	switch {
	case awsS3.Region != "":
		return newS3Writer(ctx, awsS3, backupParams, commonParams)
	case gcpStorage.BucketName != "":
		return newGcpWriter(ctx, gcpStorage, backupParams, commonParams)
	case azureBlob.ContainerName != "":
		return newAzureWriter(ctx, azureBlob, backupParams, commonParams)
	default:
		return newLocalWriter(ctx, backupParams, commonParams)
	}
}
