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

package storage

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/internal/config"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/encoding/asbx"
	ioStorage "github.com/aerospike/backup-go/io/storage"
	"github.com/aerospike/backup-go/io/storage/aws/s3"
	"github.com/aerospike/backup-go/io/storage/azure/blob"
	"github.com/aerospike/backup-go/io/storage/gcp/storage"
	"github.com/aerospike/backup-go/io/storage/local"
)

// NewBackupWriter initializes and returns a backup.Writer
// based on the provided parameters or cleans up artifacts if required.
func NewBackupWriter(
	ctx context.Context,
	params *config.BackupParams,
	sa *backup.SecretAgentConfig,
	logger *slog.Logger,
) (backup.Writer, error) {
	// We initialize a writer only if output is configured.
	writer, err := newWriter(ctx, params, sa, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup writer: %w", err)
	}

	// If asbackup was launched with --remove-artifacts, we don't need to initialize all clients.
	// We clean the folder on writer initialization and exit.
	if params.Backup != nil && params.Backup.RemoveArtifacts {
		return nil, nil
	}

	return writer, nil
}

func newWriter(
	ctx context.Context,
	params *config.BackupParams,
	sa *backup.SecretAgentConfig,
	logger *slog.Logger,
) (backup.Writer, error) {
	directory, outputFile := getDirectoryOutputFile(params)
	shouldClearTarget, continueBackup := getShouldCleanContinue(params)
	opts := newWriterOpts(directory, outputFile, shouldClearTarget, continueBackup, params.IsXDR())

	logger.Info("initializing storage for writer",
		slog.String("directory", directory),
		slog.String("output_file", outputFile),
		slog.Bool("should_clear_target", shouldClearTarget),
		slog.Bool("continue_backup", continueBackup),
	)

	switch {
	case params.AwsS3.BucketName != "":
		defer logger.Info("initialized AWS storage writer",
			slog.String("bucket", params.AwsS3.BucketName),
			slog.String("storage_class", params.AwsS3.StorageClass),
			slog.Int("chunk_size", params.AwsS3.ChunkSize),
			slog.String("endpoint", params.AwsS3.Endpoint),
		)

		if err := params.AwsS3.LoadSecrets(sa); err != nil {
			return nil, fmt.Errorf("failed to load AWS secrets: %w", err)
		}

		return newS3Writer(ctx, params.AwsS3, opts)
	case params.GcpStorage.BucketName != "":
		defer logger.Info("initialized GCP storage writer",
			slog.String("bucket", params.GcpStorage.BucketName),
			slog.Int("chunk_size", params.GcpStorage.ChunkSize),
			slog.String("endpoint", params.GcpStorage.Endpoint),
		)

		if err := params.GcpStorage.LoadSecrets(sa); err != nil {
			return nil, fmt.Errorf("failed to load GCP secrets: %w", err)
		}

		return newGcpWriter(ctx, params.GcpStorage, opts)
	case params.AzureBlob.ContainerName != "":
		defer logger.Info("initialized Azure storage writer",
			slog.String("container", params.AzureBlob.ContainerName),
			slog.String("access_tier", params.AzureBlob.AccessTier),
			slog.Int("block_size", params.AzureBlob.BlockSize),
			slog.String("endpoint", params.AzureBlob.Endpoint),
		)

		if err := params.AzureBlob.LoadSecrets(sa); err != nil {
			return nil, fmt.Errorf("failed to load azure secrets: %w", err)
		}

		return newAzureWriter(ctx, params.AzureBlob, opts)
	default:
		defer logger.Info("initialized local storage writer")
		return newLocalWriter(ctx, opts)
	}
}

func getDirectoryOutputFile(params *config.BackupParams) (directory, outputFile string) {
	if params.Backup != nil {
		return params.Common.Directory, params.Backup.OutputFile
	}
	// Xdr backup.
	return params.BackupXDR.Directory, ""
}

func getShouldCleanContinue(params *config.BackupParams) (shouldClearTarget, continueBackup bool) {
	if params.Backup != nil {
		return params.Backup.ShouldClearTarget(), params.Backup.Continue != ""
	}
	// Xdr backup.

	return params.BackupXDR.RemoveFiles, false
}

func newWriterOpts(
	directory, outputFile string,
	shouldClearTarget, continueBackup bool,
	isXDR bool,
) []ioStorage.Opt {
	opts := make([]ioStorage.Opt, 0)

	if directory != "" && outputFile == "" {
		opts = append(opts, ioStorage.WithDir(directory))
	}

	if outputFile != "" && directory == "" {
		opts = append(opts, ioStorage.WithFile(outputFile))
	}

	if shouldClearTarget {
		opts = append(opts, ioStorage.WithRemoveFiles())
	}

	if continueBackup {
		opts = append(opts, ioStorage.WithSkipDirCheck())
	}

	if isXDR {
		opts = append(opts, ioStorage.WithValidator(asbx.NewValidator()))
	} else {
		opts = append(opts, ioStorage.WithValidator(asb.NewValidator()))
	}

	return opts
}

func newLocalWriter(ctx context.Context,
	opts []ioStorage.Opt,
) (backup.Writer, error) {
	return local.NewWriter(ctx, opts...)
}

func newS3Writer(
	ctx context.Context,
	a *models.AwsS3,
	opts []ioStorage.Opt,
) (backup.Writer, error) {
	client, err := newS3Client(ctx, a)
	if err != nil {
		return nil, err
	}

	if a.StorageClass != "" {
		opts = append(opts, ioStorage.WithStorageClass(a.StorageClass))
	}

	opts = append(opts, ioStorage.WithChunkSize(a.ChunkSize))

	return s3.NewWriter(ctx, client, a.BucketName, opts...)
}

func newGcpWriter(
	ctx context.Context,
	g *models.GcpStorage,
	opts []ioStorage.Opt,
) (backup.Writer, error) {
	client, err := newGcpClient(ctx, g)
	if err != nil {
		return nil, err
	}

	opts = append(opts, ioStorage.WithChunkSize(g.ChunkSize))

	return storage.NewWriter(ctx, client, g.BucketName, opts...)
}

func newAzureWriter(
	ctx context.Context,
	a *models.AzureBlob,
	opts []ioStorage.Opt,
) (backup.Writer, error) {
	client, err := newAzureClient(a)
	if err != nil {
		return nil, err
	}

	if a.AccessTier != "" {
		opts = append(opts, ioStorage.WithAccessTier(a.AccessTier))
	}

	opts = append(opts, ioStorage.WithChunkSize(a.BlockSize))

	return blob.NewWriter(ctx, client, a.ContainerName, opts...)
}
