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
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/backup-go/io/storage"
	bModels "github.com/aerospike/backup-go/models"
	"github.com/aerospike/tools-common-go/client"
)

const idRestore = "asrestore-cli"

type ASRestore struct {
	backupClient  *backup.Client
	restoreConfig *backup.ConfigRestore

	reader    backup.StreamingReader
	xdrReader backup.StreamingReader
	// Restore Mode: auto, asb, asbx
	mode string

	isLogJSON bool

	logger *slog.Logger
}

type ASRestoreParams struct {
	App           *models.App
	ClientConfig  *client.AerospikeConfig
	ClientPolicy  *models.ClientPolicy
	RestoreParams *models.Restore
	CommonParams  *models.Common
	Compression   *models.Compression
	Encryption    *models.Encryption
	SecretAgent   *models.SecretAgent
	AwsS3         *models.AwsS3
	GcpStorage    *models.GcpStorage
	AzureBlob     *models.AzureBlob
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

	logger.Info("initializing restore config",
		slog.Any("namespace_source", *restoreConfig.Namespace.Source),
		slog.Any("namespace_destination", *restoreConfig.Namespace.Destination),
		slog.String("encryption", params.Encryption.Mode),
		slog.Int("compression", params.Compression.Level),
		slog.Any("retry", *restoreConfig.RetryPolicy),
		slog.Any("sets", restoreConfig.SetList),
		slog.Any("bins", restoreConfig.BinList),
		slog.Int("parallel", restoreConfig.Parallel),
		slog.Bool("no_records", restoreConfig.NoRecords),
		slog.Bool("no_indexes", restoreConfig.NoIndexes),
		slog.Bool("no_udfs", restoreConfig.NoUDFs),
		slog.Bool("disable_batch_writes", restoreConfig.DisableBatchWrites),
		slog.Int("batch_size", restoreConfig.BatchSize),
		slog.Int("max_asynx_batches", restoreConfig.MaxAsyncBatches),
		slog.Int64("extra_ttl", restoreConfig.ExtraTTL),
		slog.Bool("ignore_records_error", restoreConfig.IgnoreRecordError),
	)

	reader, xdrReader, err := initializeRestoreReader(ctx, params, restoreConfig.SecretAgentConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create restore reader: %w", err)
	}

	warmUp := getWarmUp(params.RestoreParams.WarmUp, params.RestoreParams.MaxAsyncBatches)
	logger.Debug("warm up is set", slog.Int("value", warmUp))

	aerospikeClient, err := newAerospikeClient(
		params.ClientConfig,
		params.ClientPolicy,
		"",
		warmUp,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike client: %w", err)
	}

	logger.Info("initializing restore client", slog.String("id", idBackup))

	backupClient, err := backup.NewClient(aerospikeClient, backup.WithLogger(logger), backup.WithID(idRestore))
	if err != nil {
		return nil, fmt.Errorf("failed to create restore client: %w", err)
	}

	return &ASRestore{
		backupClient:  backupClient,
		restoreConfig: restoreConfig,
		reader:        reader,
		xdrReader:     xdrReader,
		mode:          params.RestoreParams.Mode,
		logger:        logger,
		isLogJSON:     params.App.LogJSON,
	}, nil
}

func (r *ASRestore) Run(ctx context.Context) error {
	if r == nil {
		return nil
	}

	switch r.mode {
	case models.RestoreModeASB:
		r.logger.Info("starting asb restore")
		r.restoreConfig.EncoderType = backup.EncoderTypeASB

		h, err := r.backupClient.Restore(ctx, r.restoreConfig, r.reader)
		if err != nil {
			return fmt.Errorf("failed to start asb restore: %w", err)
		}

		go printFilesNumber(ctx, r.reader.GetNumber, models.RestoreModeASB, r.logger)
		go printRestoreEstimate(ctx, h.GetStats(), h.GetMetrics, r.reader.GetSize, r.logger)

		if err = h.Wait(ctx); err != nil {
			return fmt.Errorf("failed to asb restore: %w", err)
		}

		reportRestore(h.GetStats(), r.isLogJSON, r.logger)
	case models.RestoreModeASBX:
		r.logger.Info("starting asbx restore")
		r.restoreConfig.EncoderType = backup.EncoderTypeASBX

		hXdr, err := r.backupClient.Restore(ctx, r.restoreConfig, r.xdrReader)
		if err != nil {
			return fmt.Errorf("failed to start asbx restore: %w", err)
		}

		go printFilesNumber(ctx, r.reader.GetNumber, models.RestoreModeASBX, r.logger)
		go printRestoreEstimate(ctx, hXdr.GetStats(), hXdr.GetMetrics, r.reader.GetSize, r.logger)

		if err = hXdr.Wait(ctx); err != nil {
			return fmt.Errorf("failed to asbx restore: %w", err)
		}

		reportRestore(hXdr.GetStats(), r.isLogJSON, r.logger)
	case models.RestoreModeAuto:
		r.logger.Info("starting auto restore")
		// If one of restore operations fails, we cancel another.
		ctx, cancel := context.WithCancel(ctx)

		var (
			wg              sync.WaitGroup
			xdrStats, stats *bModels.RestoreStats
		)

		errChan := make(chan error, 2)

		if r.reader != nil {
			wg.Add(1)

			go func() {
				defer wg.Done()

				restoreCfg := *r.restoreConfig
				restoreCfg.EncoderType = backup.EncoderTypeASB

				h, err := r.backupClient.Restore(ctx, &restoreCfg, r.reader)
				if err != nil {
					errChan <- fmt.Errorf("failed to start asb restore: %w", err)

					cancel()

					return
				}

				go printFilesNumber(ctx, r.reader.GetNumber, models.RestoreModeASB, r.logger)
				go printRestoreEstimate(ctx, h.GetStats(), h.GetMetrics, r.reader.GetSize, r.logger)

				if err = h.Wait(ctx); err != nil {
					errChan <- fmt.Errorf("failed to asb restore: %w", err)

					cancel()

					return
				}

				stats = h.GetStats()
			}()
		}

		if r.xdrReader != nil {
			wg.Add(1)

			go func() {
				defer wg.Done()

				restoreXdrCfg := *r.restoreConfig
				restoreXdrCfg.EncoderType = backup.EncoderTypeASBX

				hXdr, err := r.backupClient.Restore(ctx, &restoreXdrCfg, r.xdrReader)
				if err != nil {
					errChan <- fmt.Errorf("failed to start asbx restore: %w", err)

					cancel()

					return
				}

				go printFilesNumber(ctx, r.xdrReader.GetNumber, models.RestoreModeASBX, r.logger)
				go printRestoreEstimate(ctx, hXdr.GetStats(), hXdr.GetMetrics, r.xdrReader.GetSize, r.logger)

				if err = hXdr.Wait(ctx); err != nil {
					errChan <- fmt.Errorf("failed to asbx restore: %w", err)

					cancel()

					return
				}

				xdrStats = hXdr.GetStats()
			}()
		}

		wg.Wait()
		close(errChan)

		// Return the first error encountered
		for err := range errChan {
			if err != nil {
				cancel()
				return err
			}
		}

		restStats := bModels.SumRestoreStats(xdrStats, stats)
		reportRestore(restStats, r.isLogJSON, r.logger)

		// To prevent context leaking.
		cancel()
	default:
		return fmt.Errorf("invalid mode: %s", r.mode)
	}

	return nil
}

func initializeRestoreConfigs(params *ASRestoreParams) *backup.ConfigRestore {
	return mapRestoreConfig(params)
}

func initializeRestoreReader(
	ctx context.Context,
	params *ASRestoreParams,
	sa *backup.SecretAgentConfig,
	logger *slog.Logger,
) (reader, xdrReader backup.StreamingReader, err error) {
	switch params.RestoreParams.Mode {
	case models.RestoreModeASB:
		reader, err = newReader(ctx, params, sa, false, logger)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create asb reader: %w", err)
		}

		return reader, nil, nil
	case models.RestoreModeASBX:
		xdrReader, err = newReader(ctx, params, sa, true, logger)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create asbx reader: %w", err)
		}

		return nil, xdrReader, nil
	case models.RestoreModeAuto:
		reader, err = newReader(ctx, params, sa, false, logger)

		switch {
		case errors.Is(err, storage.ErrEmptyStorage):
			reader = nil
		case err != nil:
			return nil, nil, fmt.Errorf("failed to create asb reader: %w", err)
		default:
		}

		xdrReader, err = newReader(ctx, params, sa, true, logger)

		switch {
		case errors.Is(err, storage.ErrEmptyStorage):
			xdrReader = nil
		case err != nil:
			return nil, nil, fmt.Errorf("failed to create asbx reader: %w", err)
		default:
		}

		// If both readers are nil return an error, as no files were found.
		if reader == nil && xdrReader == nil {
			return nil, nil, err
		}

		return reader, xdrReader, nil
	default:
		return nil, nil, fmt.Errorf("invalid restore mode: %s", params.RestoreParams.Mode)
	}
}

func getWarmUp(warmUp, maxAsyncBatches int) int {
	switch warmUp {
	case 0:
		return maxAsyncBatches + 1
	default:
		return warmUp
	}
}
