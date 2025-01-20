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
	"github.com/aerospike/backup-go/internal/util"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/encoding/asbx"
	"github.com/aerospike/tools-common-go/client"
)

const idRestore = "asrestore-cli"

type ASRestore struct {
	backupClient  *backup.Client
	restoreConfig *backup.ConfigRestore
	reader        backup.StreamingReader
	xdrReader     backup.StreamingReader
	// Restore Mode: auto, asb, asbx
	mode string
}

type ASRestoreParams struct {
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

	reader, xdrReader, err := initializeRestoreReader(ctx, params, restoreConfig.SecretAgentConfig)
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
		xdrReader:     xdrReader,
		mode:          params.RestoreParams.Mode,
	}, nil
}

func (r *ASRestore) Run(ctx context.Context) error {
	if r == nil {
		return nil
	}

	switch r.mode {
	case models.RestoreModeASB:
		r.restoreConfig.EncoderType = backup.EncoderTypeASB

		h, err := r.backupClient.Restore(ctx, r.restoreConfig, r.reader)
		if err != nil {
			return fmt.Errorf("failed to start asb restore: %w", err)
		}

		if err = h.Wait(ctx); err != nil {
			return fmt.Errorf("failed to asb restore: %w", err)
		}

		printRestoreReport(reportHeaderRestore, h.GetStats())
	case models.RestoreModeASBX:
		r.restoreConfig.EncoderType = backup.EncoderTypeASBX

		hXdr, err := r.backupClient.Restore(ctx, r.restoreConfig, r.xdrReader)
		if err != nil {
			return fmt.Errorf("failed to start asbx restore: %w", err)
		}

		if err = hXdr.Wait(ctx); err != nil {
			return fmt.Errorf("failed to asbx restore: %w", err)
		}

		printRestoreReport(reportHeaderRestoreXDR, hXdr.GetStats())
	case models.RestoreModeAuto:
		// If one of restore operations fails, we cancel another.
		ctx, cancel := context.WithCancel(ctx)
		// We should copy config to new variable, not to overwrite encoder.
		restoreCfg := *r.restoreConfig
		restoreCfg.EncoderType = backup.EncoderTypeASB

		h, err := r.backupClient.Restore(ctx, &restoreCfg, r.reader)
		if err != nil {
			cancel()
			return fmt.Errorf("failed to start asb restore: %w", err)
		}

		restoreXdrCfg := *r.restoreConfig
		restoreXdrCfg.EncoderType = backup.EncoderTypeASBX

		hXdr, err := r.backupClient.Restore(ctx, &restoreXdrCfg, r.xdrReader)
		if err != nil {
			cancel()
			return fmt.Errorf("failed to start asbx restore: %w", err)
		}

		if err = h.Wait(ctx); err != nil {
			cancel()
			return fmt.Errorf("failed to asb restore: %w", err)
		}

		if err = hXdr.Wait(ctx); err != nil {
			cancel()
			return fmt.Errorf("failed to asbx restore: %w", err)
		}

		printRestoreReport(reportHeaderRestore, h.GetStats())
		fmt.Println() // For pretty print.
		printRestoreReport(reportHeaderRestoreXDR, hXdr.GetStats())
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

func initializeRestoreReader(ctx context.Context, params *ASRestoreParams, sa *backup.SecretAgentConfig,
) (reader, xdrReader backup.StreamingReader, err error) {
	switch params.RestoreParams.Mode {
	case models.RestoreModeASB:
		reader, err = newReader(ctx, params, sa, false)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create asb reader: %w", err)
		}

		return reader, nil, nil
	case models.RestoreModeASBX:
		xdrReader, err = newReader(ctx, params, sa, true)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create asbx reader: %w", err)
		}

		return nil, xdrReader, nil
	case models.RestoreModeAuto:
		reader, err = newReader(ctx, params, sa, false)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create asb reader: %w", err)
		}
		// List all files first.
		list, err := reader.ListObjects(ctx, params.CommonParams.Directory)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to list objects: %w", err)
		}
		// Separate each file type for different lists.
		asbList, asbxList, err := splitList(list)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to split objects: %w", err)
		}

		if len(asbxList) == 0 && len(asbList) == 0 {
			return nil, nil, fmt.Errorf("no asb or asbx file found in: %s",
				params.CommonParams.Directory)
		}

		// Load ASB files for reading.
		reader.SetObjectsToStream(asbList)

		xdrReader, err = newReader(ctx, params, sa, true)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create asbx reader: %w", err)
		}
		// Load ASBX files for reading.
		xdrReader.SetObjectsToStream(asbxList)

		return reader, xdrReader, nil
	default:
		return nil, nil, fmt.Errorf("invalid restore mode: %s", params.RestoreParams.Mode)
	}
}

// splitList splits one file list to 2 lists for asb and for asbx restore.
func splitList(list []string) (asbList, asbxList []string, err error) {
	asbValidator := asb.NewValidator()
	asbxValidator := asbx.NewValidator()

	for i := range list {
		// If valid for asb, append to asbList.
		if err := asbValidator.Run(list[i]); err == nil {
			asbList = append(asbList, list[i])
			continue
		}
		// If valid for asbx, append to asbxList.
		if err := asbxValidator.Run(list[i]); err == nil {
			asbxList = append(asbxList, list[i])
		}
	}

	if len(asbxList) > 0 {
		// We sort asbx files by prefix and suffix to restore them in the correct order.
		asbxList, err = util.SortBackupFiles(asbxList)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to sort asbx files: %w", err)
		}
	}

	return asbList, asbxList, nil
}
