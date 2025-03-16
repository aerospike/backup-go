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
	"strings"

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/backup-go/internal/asinfo"
	bModels "github.com/aerospike/backup-go/models"
	"github.com/aerospike/tools-common-go/client"
)

const (
	idBackup            = "asbackup-cli"
	xdrSupportedVersion = 8
)

type ASBackup struct {
	backupClient    *backup.Client
	backupConfig    *backup.ConfigBackup
	backupConfigXDR *backup.ConfigBackupXDR

	writer backup.Writer
	// reader is used to read a state file.
	reader backup.StreamingReader

	// Additional params.
	isEstimate       bool
	estimatesSamples int64
}

// ASBackupParams params wrapper for clean code.
type ASBackupParams struct {
	ClientConfig    *client.AerospikeConfig
	ClientPolicy    *models.ClientPolicy
	BackupParams    *models.Backup
	BackupXDRParams *models.BackupXDR
	CommonParams    *models.Common
	Compression     *models.Compression
	Encryption      *models.Encryption
	SecretAgent     *models.SecretAgent
	AwsS3           *models.AwsS3
	GcpStorage      *models.GcpStorage
	AzureBlob       *models.AzureBlob
}

func (a *ASBackupParams) isXDR() bool {
	return a.BackupXDRParams != nil && a.BackupParams == nil
}

func (a *ASBackupParams) isStopXDR() bool {
	return a.BackupXDRParams != nil && a.BackupXDRParams.StopXDR
}

func (a *ASBackupParams) isUnblockMRT() bool {
	return a.BackupXDRParams != nil && a.BackupXDRParams.UnblockMRT
}

func (a *ASBackupParams) SkipWriterInit() bool {
	if a.BackupParams != nil {
		return !a.BackupParams.Estimate
	}

	return true
}

func NewASBackup(
	ctx context.Context,
	params *ASBackupParams,
	logger *slog.Logger,
) (*ASBackup, error) {
	// Validations.
	if err := validateBackup(params); err != nil {
		return nil, err
	}

	// Initializations.
	backupConfig, backupXDRConfig, err := initializeBackupConfigs(params)
	if err != nil {
		return nil, err
	}

	secretAgent := getSecretAgent(backupConfig, backupXDRConfig)

	// We don't need writer for estimates.
	var writer backup.Writer
	if params.SkipWriterInit() {
		writer, err = initializeBackupWriter(ctx, params, secretAgent)
		if err != nil {
			return nil, err
		}

		// For --remove-artifacts we shouldn't start backup.
		if writer == nil {
			return nil, nil
		}
	}

	reader, err := initializeBackupReader(ctx, params, secretAgent, logger)
	if err != nil {
		return nil, err
	}

	var racks string
	if params.BackupParams != nil {
		racks = params.BackupParams.PreferRacks
	}

	aerospikeClient, err := newAerospikeClient(params.ClientConfig, params.ClientPolicy, racks, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike client: %w", err)
	}

	if params.BackupXDRParams != nil {
		infoClient := asinfo.NewInfoClientFromAerospike(
			aerospikeClient,
			backupXDRConfig.InfoPolicy,
			backupXDRConfig.InfoRetryPolicy,
		)

		version, err := infoClient.GetVersion()
		if err != nil {
			return nil, fmt.Errorf("failed to get version: %w", err)
		}

		if version.Major < xdrSupportedVersion {
			return nil, fmt.Errorf("version %s is unsupported, only databse version %d+ is supproted",
				version.String(), xdrSupportedVersion)
		}

		// Stop xdr.
		if params.isStopXDR() {
			logger.Info("stopping XDR on the database")

			if err = stopXDR(infoClient, backupXDRConfig.DC, backupXDRConfig.Namespace); err != nil {
				return nil, fmt.Errorf("failed to stop XDR: %w", err)
			}

			return nil, nil
		}

		// Unblock mRT.
		if params.isUnblockMRT() {
			logger.Info("enabling MRT writes on the database")

			if err = unblockMrt(infoClient, backupXDRConfig.Namespace); err != nil {
				return nil, fmt.Errorf("failed to enable MRT writes: %w", err)
			}

			return nil, nil
		}
	}

	backupClient, err := backup.NewClient(aerospikeClient, backup.WithLogger(logger), backup.WithID(idBackup))
	if err != nil {
		return nil, fmt.Errorf("failed to create backup client: %w", err)
	}

	asb := &ASBackup{
		backupClient:    backupClient,
		backupConfig:    backupConfig,
		backupConfigXDR: backupXDRConfig,
		writer:          writer,
		reader:          reader,
	}

	if params.BackupParams != nil {
		asb.isEstimate = params.BackupParams.Estimate
		asb.estimatesSamples = params.BackupParams.EstimateSamples
	}

	return asb, nil
}

func initializeBackupConfigs(params *ASBackupParams) (*backup.ConfigBackup, *backup.ConfigBackupXDR, error) {
	var (
		backupConfig    *backup.ConfigBackup
		backupXDRConfig *backup.ConfigBackupXDR
		err             error
	)

	switch {
	case !params.isXDR():
		backupConfig, err = mapBackupConfig(params)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to map backup config: %w", err)
		}
	case params.isXDR():
		backupXDRConfig = mapBackupXDRConfig(params)

		// On xdr backup we backup only uds and indexes.
		backupConfig = backup.NewDefaultBackupConfig()

		backupConfig.NoRecords = true
		backupConfig.Namespace = backupXDRConfig.Namespace
	}

	return backupConfig, backupXDRConfig, nil
}

func initializeBackupReader(
	ctx context.Context, params *ASBackupParams,
	sa *backup.SecretAgentConfig,
	logger *slog.Logger,
) (backup.StreamingReader, error) {
	if params.BackupParams == nil {
		return nil, nil
	}

	if !params.BackupParams.ShouldSaveState() {
		return nil, nil
	}

	// Fill params to load a state file.
	restoreParams := &ASRestoreParams{
		RestoreParams: &models.Restore{
			InputFile: params.BackupParams.StateFileDst,
		},
		CommonParams: &models.Common{},
	}

	reader, err := newReader(ctx, restoreParams, sa, false, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}

	return reader, nil
}

func initializeBackupWriter(ctx context.Context, params *ASBackupParams, sa *backup.SecretAgentConfig,
) (backup.Writer, error) {
	// We initialize a writer only if output is configured.
	writer, err := newWriter(ctx, params, sa)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup writer: %w", err)
	}

	// If asbackup was launched with --remove-artifacts, we don't need to initialize all clients.
	// We clean the folder on writer initialization and exit.
	if params.BackupParams != nil && params.BackupParams.RemoveArtifacts {
		return nil, nil
	}

	return writer, nil
}

func (b *ASBackup) Run(ctx context.Context) error {
	// If asbackup was called with --remove-artifacts, it would be nil.
	if b == nil {
		return nil
	}

	switch {
	case b.isEstimate:
		// Calculating estimates.
		estimates, err := b.backupClient.Estimate(ctx, b.backupConfig, b.estimatesSamples)
		if err != nil {
			return fmt.Errorf("failed to calculate backup estimate: %w", err)
		}

		printEstimateReport(estimates)
	case b.backupConfigXDR != nil:
		// Running xdr backup.
		hXdr, err := b.backupClient.BackupXDR(ctx, b.backupConfigXDR, b.writer)
		if err != nil {
			return fmt.Errorf("failed to start xdr backup: %w", err)
		}
		// Backup indexes and udfs.
		h, err := b.backupClient.Backup(ctx, b.backupConfig, b.writer, b.reader)
		if err != nil {
			return fmt.Errorf("failed to start backup of indexes and udfs: %w", err)
		}

		if err = hXdr.Wait(ctx); err != nil {
			return fmt.Errorf("failed to xdr backup: %w", err)
		}

		if err = h.Wait(ctx); err != nil {
			return fmt.Errorf("failed to backup indexes and udfs: %w", err)
		}

		stats := bModels.SumBackupStats(h.GetStats(), hXdr.GetStats())
		printBackupReport(stats, true)
	default:
		// Running ordinary backup.
		h, err := b.backupClient.Backup(ctx, b.backupConfig, b.writer, b.reader)
		if err != nil {
			return fmt.Errorf("failed to start backup: %w", err)
		}

		if err = h.Wait(ctx); err != nil {
			return fmt.Errorf("failed to backup: %w", err)
		}

		printBackupReport(h.GetStats(), false)
	}

	return nil
}

func getSecretAgent(b *backup.ConfigBackup, bxdr *backup.ConfigBackupXDR) *backup.SecretAgentConfig {
	switch {
	case b != nil:
		return b.SecretAgentConfig
	case bxdr != nil:
		return bxdr.SecretAgentConfig
	default:
		return nil
	}
}

func stopXDR(infoClient *asinfo.InfoClient, dc, namespace string) error {
	nodes := infoClient.GetNodesNames()

	for _, node := range nodes {
		// Check before stopping if DC exists.
		_, err := infoClient.GetStats(node, dc, namespace)
		if err != nil {
			if strings.Contains(err.Error(), "DC not found") {
				continue
			}

			return fmt.Errorf("failed to get stats for node %s: %w", node, err)
		}

		if err = infoClient.StopXDR(node, dc); err != nil {
			return fmt.Errorf("failed to stop XDR on node %s: %w", node, err)
		}
	}

	return nil
}

func unblockMrt(infoClient *asinfo.InfoClient, namespace string) error {
	nodes := infoClient.GetNodesNames()

	for _, node := range nodes {
		if err := infoClient.UnBlockMRTWrites(node, namespace); err != nil {
			return fmt.Errorf("failed to unblock mrts on node %s: %w", node, err)
		}
	}

	return nil
}
