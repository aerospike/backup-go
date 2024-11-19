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

const idRestore = "asrestore-cli"

type ASRestore struct {
	backupClient  *backup.Client
	restoreConfig *backup.RestoreConfig
	reader        backup.StreamingReader
}

func NewASRestore(
	ctx context.Context,
	clientConfig *client.AerospikeConfig,
	clientPolicy *models.ClientPolicy,
	restoreParams *models.Restore,
	commonParams *models.Common,
	compression *models.Compression,
	encryption *models.Encryption,
	secretAgent *models.SecretAgent,
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
	logger *slog.Logger,
) (*ASRestore, error) {
	if err := validateStorages(awsS3, gcpStorage, azureBlob); err != nil {
		return nil, err
	}

	if err := validateCommonParams(commonParams); err != nil {
		return nil, err
	}

	restoreConfig, err := mapRestoreConfig(
		restoreParams,
		commonParams,
		compression,
		encryption,
		secretAgent,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create restore config: %w", err)
	}

	reader, err := getReader(
		ctx,
		restoreParams,
		commonParams,
		awsS3,
		gcpStorage,
		azureBlob,
		nil,
		restoreConfig.SecretAgentConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup reader: %w", err)
	}

	aerospikeClient, err := newAerospikeClient(clientConfig, clientPolicy, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike client: %w", err)
	}

	backupClient, err := backup.NewClient(aerospikeClient, backup.WithLogger(logger), backup.WithID(idRestore))
	if err != nil {
		return nil, fmt.Errorf("failed to create backup client: %w", err)
	}

	return &ASRestore{
		backupClient:  backupClient,
		restoreConfig: restoreConfig,
		reader:        reader,
	}, nil
}

func (r *ASRestore) Run(ctx context.Context) error {
	if r == nil {
		return nil
	}

	h, err := r.backupClient.Restore(ctx, r.restoreConfig, r.reader)
	if err != nil {
		return fmt.Errorf("failed to start restore: %w", err)
	}

	if err := h.Wait(ctx); err != nil {
		return fmt.Errorf("failed to restore: %w", err)
	}

	printRestoreReport(h.GetStats())

	return nil
}
