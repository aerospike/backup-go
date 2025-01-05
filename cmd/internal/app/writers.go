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

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/backup-go/io/aws/s3"
	"github.com/aerospike/backup-go/io/azure/blob"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/gcp/storage"
	"github.com/aerospike/backup-go/io/local"
)

func newWriter(
	ctx context.Context,
	params *ASBackupParams,
	sa *backup.SecretAgentConfig,
) (backup.Writer, error) {
	directory, outputFile := getDirectoryOutputFile(params)
	shouldClearTarget, continueBackup := getShouldCleanContinue(params)

	switch {
	case params.AwsS3.Region != "":
		if err := params.AwsS3.LoadSecrets(sa); err != nil {
			return nil, fmt.Errorf("failed to load AWS secrets: %w", err)
		}

		return newS3Writer(ctx, params.AwsS3, directory, outputFile, shouldClearTarget, continueBackup)
	case params.GcpStorage.BucketName != "":
		if err := params.GcpStorage.LoadSecrets(sa); err != nil {
			return nil, fmt.Errorf("failed to load GCP secrets: %w", err)
		}

		return newGcpWriter(ctx, params.GcpStorage, directory, outputFile, shouldClearTarget, continueBackup)
	case params.AzureBlob.ContainerName != "":
		if err := params.AzureBlob.LoadSecrets(sa); err != nil {
			return nil, fmt.Errorf("failed to load azure secrets: %w", err)
		}

		return newAzureWriter(ctx, params.AzureBlob, directory, outputFile, shouldClearTarget, continueBackup)
	default:
		return newLocalWriter(ctx, directory, outputFile, shouldClearTarget, continueBackup)
	}
}

func getDirectoryOutputFile(params *ASBackupParams) (directory, outputFile string) {
	if params.BackupParams != nil {
		return params.CommonParams.Directory, params.BackupParams.OutputFile
	}
	// Xdr backup.
	return params.BackupXDRParams.Directory, ""
}

func getShouldCleanContinue(params *ASBackupParams) (shouldClearTarget, continueBackup bool) {
	if params.BackupParams != nil {
		return params.BackupParams.ShouldClearTarget(), params.BackupParams.Continue != ""
	}
	// Xdr backup.
	return false, false
}

func newLocalWriter(ctx context.Context,
	directory, outputFile string,
	shouldClearTarget, continueBackup bool,
) (backup.Writer, error) {
	var opts []local.Opt

	if directory != "" && outputFile == "" {
		opts = append(opts, local.WithDir(directory))
	}

	if outputFile != "" && directory == "" {
		opts = append(opts, local.WithFile(outputFile))
	}

	if shouldClearTarget {
		opts = append(opts, local.WithRemoveFiles())
	}

	if continueBackup {
		opts = append(opts, local.WithSkipDirCheck())
	}

	opts = append(opts, local.WithValidator(asb.NewValidator()))

	return local.NewWriter(ctx, opts...)
}

func newS3Writer(
	ctx context.Context,
	a *models.AwsS3,
	directory, outputFile string,
	shouldClearTarget, continueBackup bool,
) (backup.Writer, error) {
	client, err := newS3Client(ctx, a)
	if err != nil {
		return nil, err
	}

	opts := make([]s3.Opt, 0)

	if directory != "" && outputFile == "" {
		opts = append(opts, s3.WithDir(directory))
	}

	if outputFile != "" && directory == "" {
		opts = append(opts, s3.WithFile(outputFile))
	}

	if shouldClearTarget {
		opts = append(opts, s3.WithRemoveFiles())
	}

	if continueBackup {
		opts = append(opts, s3.WithSkipDirCheck())
	}

	opts = append(opts, s3.WithValidator(asb.NewValidator()))

	return s3.NewWriter(ctx, client, a.BucketName, opts...)
}

func newGcpWriter(
	ctx context.Context,
	g *models.GcpStorage,
	directory, outputFile string,
	shouldClearTarget, continueBackup bool,
) (backup.Writer, error) {
	client, err := newGcpClient(ctx, g)
	if err != nil {
		return nil, err
	}

	opts := make([]storage.Opt, 0)

	if directory != "" && outputFile == "" {
		opts = append(opts, storage.WithDir(directory), storage.WithValidator(asb.NewValidator()))
	}

	if outputFile != "" && directory == "" {
		opts = append(opts, storage.WithFile(outputFile))
	}

	if shouldClearTarget {
		opts = append(opts, storage.WithRemoveFiles())
	}

	if continueBackup {
		opts = append(opts, storage.WithSkipDirCheck())
	}

	opts = append(opts, storage.WithValidator(asb.NewValidator()))

	return storage.NewWriter(ctx, client, g.BucketName, opts...)
}

func newAzureWriter(
	ctx context.Context,
	a *models.AzureBlob,
	directory, outputFile string,
	shouldClearTarget, continueBackup bool,
) (backup.Writer, error) {
	client, err := newAzureClient(a)
	if err != nil {
		return nil, err
	}

	opts := make([]blob.Opt, 0)

	if directory != "" && outputFile == "" {
		opts = append(opts, blob.WithDir(directory), blob.WithValidator(asb.NewValidator()))
	}

	if outputFile != "" && directory == "" {
		opts = append(opts, blob.WithFile(outputFile))
	}

	if shouldClearTarget {
		opts = append(opts, blob.WithRemoveFiles())
	}

	if continueBackup {
		opts = append(opts, blob.WithSkipDirCheck())
	}

	opts = append(opts, blob.WithValidator(asb.NewValidator()))

	return blob.NewWriter(ctx, client, a.ContainerName, opts...)
}
