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

package restore

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/internal/config"
	"github.com/aerospike/backup-go/cmd/internal/logging"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/backup-go/cmd/internal/storage"
	bModels "github.com/aerospike/backup-go/models"
)

const idRestore = "asrestore-cli"

// Service represents a type used to handle Aerospike data recovery operations with configurable restore settings.
type Service struct {
	backupClient  *backup.Client
	restoreConfig *backup.ConfigRestore

	reader    backup.StreamingReader
	xdrReader backup.StreamingReader
	// Restore Mode: auto, asb, asbx
	mode string

	isLogJSON bool

	logger *slog.Logger
}

// NewService initializes and returns a new Service instance,
// configuring all necessary components for a restore process.
func NewService(
	ctx context.Context,
	params *config.RestoreParams,
	logger *slog.Logger,
) (*Service, error) {
	var (
		aerospikeClient *a.Client
		err             error
	)

	// Validations.
	if err := config.ValidateRestore(params); err != nil {
		return nil, err
	}

	// Initializations.
	logger.Info("initializing restore config")

	restoreConfig := config.NewRestoreConfig(params)

	// Skip this part on validation.
	if !restoreConfig.ValidateOnly {
		logger.Info("initialized restore config",
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

		warmUp := GetWarmUp(params.Restore.WarmUp, params.Restore.MaxAsyncBatches)
		logger.Debug("warm up is set", slog.Int("value", warmUp))

		aerospikeClient, err = storage.NewAerospikeClient(
			params.ClientConfig,
			params.ClientPolicy,
			"",
			warmUp,
			logger,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create aerospike client: %w", err)
		}
	}

	reader, xdrReader, err := storage.NewRestoreReader(ctx, params, restoreConfig.SecretAgentConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create restore reader: %w", err)
	}

	logger.Info("initializing restore client", slog.String("id", idRestore))

	backupClient, err := backup.NewClient(aerospikeClient, backup.WithLogger(logger), backup.WithID(idRestore))
	if err != nil {
		return nil, fmt.Errorf("failed to create restore client: %w", err)
	}

	return &Service{
		backupClient:  backupClient,
		restoreConfig: restoreConfig,
		reader:        reader,
		xdrReader:     xdrReader,
		mode:          params.Restore.Mode,
		logger:        logger,
		isLogJSON:     params.App.LogJSON,
	}, nil
}

// Run executes the restore process based on the configured mode, handling ASB, ASBX, or Auto restore modes.
func (r *Service) Run(ctx context.Context) error {
	if r == nil {
		return nil
	}

	switch r.mode {
	case models.RestoreModeASB, models.RestoreModeAuto:
		r.logger.Info("starting asb restore")
		r.restoreConfig.EncoderType = backup.EncoderTypeASB

		h, err := r.backupClient.Restore(ctx, r.restoreConfig, r.reader)
		if err != nil {
			return fmt.Errorf("failed to start asb restore: %w", err)
		}

		go logging.PrintFilesNumber(ctx, r.reader.GetNumber, models.RestoreModeASB, r.logger)
		go logging.PrintRestoreEstimate(ctx, h.GetStats(), h.GetMetrics, r.reader.GetSize, r.logger)

		if err = h.Wait(ctx); err != nil {
			return fmt.Errorf("failed to asb restore: %w", err)
		}

		logging.ReportRestore(h.GetStats(), r.isLogJSON, r.logger)
	case models.RestoreModeASBX:
		r.logger.Info("starting asbx restore")
		r.restoreConfig.EncoderType = backup.EncoderTypeASBX

		hXdr, err := r.backupClient.Restore(ctx, r.restoreConfig, r.xdrReader)
		if err != nil {
			return fmt.Errorf("failed to start asbx restore: %w", err)
		}

		go logging.PrintFilesNumber(ctx, r.reader.GetNumber, models.RestoreModeASBX, r.logger)
		go logging.PrintRestoreEstimate(ctx, hXdr.GetStats(), hXdr.GetMetrics, r.reader.GetSize, r.logger)

		if err = hXdr.Wait(ctx); err != nil {
			return fmt.Errorf("failed to asbx restore: %w", err)
		}

		logging.ReportRestore(hXdr.GetStats(), r.isLogJSON, r.logger)
	default:
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

				go logging.PrintFilesNumber(ctx, r.reader.GetNumber, models.RestoreModeASB, r.logger)
				go logging.PrintRestoreEstimate(ctx, h.GetStats(), h.GetMetrics, r.reader.GetSize, r.logger)

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

				go logging.PrintFilesNumber(ctx, r.xdrReader.GetNumber, models.RestoreModeASBX, r.logger)
				go logging.PrintRestoreEstimate(ctx, hXdr.GetStats(), hXdr.GetMetrics, r.xdrReader.GetSize, r.logger)

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
		logging.ReportRestore(restStats, r.isLogJSON, r.logger)

		// To prevent context leaking.
		cancel()
	}

	return nil
}

// GetWarmUp calculates and returns the warm-up value based on the provided warmUp and maxAsyncBatches parameters.
// If warmUp is 0, it returns one greater than maxAsyncBatches. Otherwise, it returns the warmUp value.
func GetWarmUp(warmUp, maxAsyncBatches int) int {
	switch warmUp {
	case 0:
		return maxAsyncBatches + 1
	default:
		return warmUp
	}
}
