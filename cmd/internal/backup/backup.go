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

package backup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/internal/config"
	"github.com/aerospike/backup-go/cmd/internal/logging"
	"github.com/aerospike/backup-go/cmd/internal/storage"
	"github.com/aerospike/backup-go/internal/asinfo"
	bModels "github.com/aerospike/backup-go/models"
)

const (
	idBackup            = "asbackup-cli"
	xdrSupportedVersion = 8
)

// Service represents a struct that encapsulates components for backup and logging functionalities.
// It is responsible for managing backup clients, configurations,
// and related components with additional operational parameters.
type Service struct {
	backupClient    *backup.Client
	backupConfig    *backup.ConfigBackup
	backupConfigXDR *backup.ConfigBackupXDR

	writer backup.Writer
	// reader is used to read a state file.
	reader backup.StreamingReader

	// Additional params.
	isEstimate       bool
	estimatesSamples int64

	isLogJSON bool

	logger *slog.Logger
}

// NewService initializes and returns a new Service instance,
// configuring all necessary components for a backup process.
func NewService(
	ctx context.Context,
	params *config.BackupParams,
	logger *slog.Logger,
) (*Service, error) {
	// Validations.
	if err := params.Backup.Validate(); err != nil {
		return nil, err
	}

	if err := params.BackupXDR.Validate(); err != nil {
		return nil, err
	}

	if err := config.ValidateStorages(params.AwsS3, params.GcpStorage, params.AzureBlob); err != nil {
		return nil, err
	}

	// Initializations.
	backupConfig, backupXDRConfig, err := config.NewBackupConfigs(params, logger)
	if err != nil {
		return nil, err
	}

	secretAgent := config.GetSecretAgent(backupConfig, backupXDRConfig)

	// We don't need a writer for estimates.
	var writer backup.Writer
	if params.SkipWriterInit() {
		writer, err = storage.NewBackupWriter(ctx, params, secretAgent, logger)
		if err != nil {
			return nil, err
		}

		// For --remove-artifacts we shouldn't start backup.
		if writer == nil {
			return nil, nil
		}
	}

	reader, err := storage.NewStateReader(ctx, params, secretAgent, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize state reader: %w", err)
	}

	var racks string
	if params.Backup != nil {
		racks = params.Backup.PreferRacks
	}

	aerospikeClient, err := storage.NewAerospikeClient(params.ClientConfig, params.ClientPolicy, racks, 0, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike client: %w", err)
	}

	if params.BackupXDR != nil {
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
		if params.IsStopXDR() {
			logger.Info("stopping XDR on the database")

			if err = stopXDR(infoClient, backupXDRConfig.DC, backupXDRConfig.Namespace); err != nil {
				return nil, fmt.Errorf("failed to stop XDR: %w", err)
			}

			return nil, nil
		}

		// Unblock mRT.
		if params.IsUnblockMRT() {
			logger.Info("enabling MRT writes on the database")

			if err = unblockMrt(infoClient, backupXDRConfig.Namespace); err != nil {
				return nil, fmt.Errorf("failed to enable MRT writes: %w", err)
			}

			return nil, nil
		}
	}

	logger.Info("initializing backup client", slog.String("id", idBackup))

	backupClient, err := backup.NewClient(aerospikeClient, backup.WithLogger(logger), backup.WithID(idBackup))
	if err != nil {
		return nil, fmt.Errorf("failed to create backup client: %w", err)
	}

	asb := &Service{
		backupClient:    backupClient,
		backupConfig:    backupConfig,
		backupConfigXDR: backupXDRConfig,
		writer:          writer,
		reader:          reader,
		logger:          logger,
		isLogJSON:       params.App.LogJSON,
	}

	if params.Backup != nil {
		asb.isEstimate = params.Backup.Estimate
		asb.estimatesSamples = params.Backup.EstimateSamples
	}

	return asb, nil
}

// Run executes the backup process for the Service based on its configuration and context,
// handling both XDR and scan backups.
// Returns an error if the backup process encounters issues.
func (s *Service) Run(ctx context.Context) error {
	// If asbackup was called with --remove-artifacts, it would be nil.
	if s == nil {
		return nil
	}

	switch {
	case s.isEstimate:
		s.logger.Info("calculating backup estimate")
		// Calculating estimates.
		estimates, err := s.backupClient.Estimate(ctx, s.backupConfig, s.estimatesSamples)
		if err != nil {
			return fmt.Errorf("failed to calculate backup estimate: %w", err)
		}

		logging.ReportEstimate(estimates, s.isLogJSON, s.logger)
	case s.backupConfigXDR != nil:
		s.logger.Info("starting xdr backup")
		// Running xdr backup.
		hXdr, err := s.backupClient.BackupXDR(ctx, s.backupConfigXDR, s.writer)
		if err != nil {
			return fmt.Errorf("failed to start xdr backup: %w", err)
		}
		// Backup indexes and udfs.
		h, err := s.backupClient.Backup(ctx, s.backupConfig, s.writer, s.reader)
		if err != nil {
			return fmt.Errorf("failed to start backup of indexes and udfs: %w", err)
		}

		go logging.PrintBackupEstimate(ctx, hXdr.GetStats(), hXdr.GetMetrics, s.logger)

		if err = hXdr.Wait(ctx); err != nil {
			return fmt.Errorf("failed to xdr backup: %w", err)
		}

		if err = h.Wait(ctx); err != nil {
			return fmt.Errorf("failed to backup indexes and udfs: %w", err)
		}

		stats := bModels.SumBackupStats(h.GetStats(), hXdr.GetStats())
		logging.ReportBackup(stats, true, s.isLogJSON, s.logger)
	default:
		s.logger.Info("starting scan backup")
		// Running ordinary backup.
		h, err := s.backupClient.Backup(ctx, s.backupConfig, s.writer, s.reader)
		if err != nil {
			return fmt.Errorf("failed to start backup: %w", err)
		}

		go logging.PrintBackupEstimate(ctx, h.GetStats(), h.GetMetrics, s.logger)

		if err = h.Wait(ctx); err != nil {
			return fmt.Errorf("failed to backup: %w", err)
		}

		logging.ReportBackup(h.GetStats(), false, s.isLogJSON, s.logger)
	}

	return nil
}

func stopXDR(infoClient *asinfo.InfoClient, dc, namespace string) error {
	nodes := infoClient.GetNodesNames()

	var errs []error

	for _, node := range nodes {
		// Check before stopping if DC exists.
		_, err := infoClient.GetStats(node, dc, namespace)
		if err != nil && strings.Contains(err.Error(), "DC not found") {
			continue
		}

		if err = infoClient.StopXDR(node, dc); err != nil {
			errs = append(errs, fmt.Errorf("failed to stop XDR on node %s: %w", node, err))
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func unblockMrt(infoClient *asinfo.InfoClient, namespace string) error {
	nodes := infoClient.GetNodesNames()

	var errs []error

	for _, node := range nodes {
		if err := infoClient.UnBlockMRTWrites(node, namespace); err != nil {
			errs = append(errs, fmt.Errorf("failed to unblock mrts on node %s: %w", node, err))
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}
