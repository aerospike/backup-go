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

type ASRestoreParams struct {
	ClientConfig     *client.AerospikeConfig
	ClientPolicy     *models.ClientPolicy
	RestoreParams    *models.Restore
	RestoreXDRParams *models.RestoreXDR
	CommonParams     *models.Common
	Compression      *models.Compression
	Encryption       *models.Encryption
	SecretAgent      *models.SecretAgent
	AwsS3            *models.AwsS3
	GcpStorage       *models.GcpStorage
	AzureBlob        *models.AzureBlob
}

func (a *ASRestoreParams) isXDR() bool {
	return a.RestoreXDRParams != nil && a.RestoreParams == nil
}

func NewASRestore(
	ctx context.Context,
	params *ASRestoreParams,
	logger *slog.Logger,
) (*ASRestore, error) {
	// Validations.
	if err := validateRestore(params); err != nil {
		return nil, err
	}

	// Initializations.
	restoreConfig := initializeRestoreConfigs(params)

	reader, err := initializeRestoreReader(ctx, params, restoreConfig.SecretAgentConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup reader: %w", err)
	}

	aerospikeClient, err := newAerospikeClient(params.ClientConfig, params.ClientPolicy, "")
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

	switch r.restoreConfig.EncoderType {
	case backup.EncoderTypeASB:
		h, err := r.backupClient.Restore(ctx, r.restoreConfig, r.reader)
		if err != nil {
			return fmt.Errorf("failed to start restore: %w", err)
		}

		if err = h.Wait(ctx); err != nil {
			return fmt.Errorf("failed to restore: %w", err)
		}

		printRestoreReport(h.GetStats())
	case backup.EncoderTypeASBX:
		h, err := r.backupClient.RestoreXDR(ctx, r.restoreConfig, r.reader)
		if err != nil {
			return fmt.Errorf("failed to start xdr restore: %w", err)
		}

		if err = h.Wait(ctx); err != nil {
			return fmt.Errorf("failed to xdr restore: %w", err)
		}

		printRestoreReport(h.GetStats())
	}

	return nil
}

func initializeRestoreConfigs(params *ASRestoreParams) *backup.RestoreConfig {
	switch {
	case !params.isXDR():
		return mapRestoreConfig(params)
	case params.isXDR():
		return mapRestoreXDRConfig(params)
	default:
		return nil
	}
}

func initializeRestoreReader(ctx context.Context, params *ASRestoreParams, sa *backup.SecretAgentConfig,
) (backup.StreamingReader, error) {
	reader, err := getReader(
		ctx,
		params.RestoreParams,
		params.CommonParams,
		params.AwsS3,
		params.GcpStorage,
		params.AzureBlob,
		nil,
		sa,
		params.isXDR(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}

	return reader, nil
}
