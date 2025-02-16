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
	"github.com/aerospike/backup-go/io/encoding/asb"
	ioStorage "github.com/aerospike/backup-go/io/storage"
	"github.com/aerospike/backup-go/io/storage/aws/s3"
	"github.com/aerospike/backup-go/io/storage/azure/blob"
	"github.com/aerospike/backup-go/io/storage/gcp/storage"
	"github.com/aerospike/backup-go/io/storage/local"
)

func newWriter(
	ctx context.Context,
	params *ASBackupParams,
	sa *backup.SecretAgentConfig,
) (backup.Writer, error) {
	directory, outputFile := getDirectoryOutputFile(params)
	shouldClearTarget, continueBackup := getShouldCleanContinue(params)
	opts := newWriterOpts(directory, outputFile, shouldClearTarget, continueBackup)

	switch {
	case params.AwsS3.Region != "":
		if err := params.AwsS3.LoadSecrets(sa); err != nil {
			return nil, fmt.Errorf("failed to load AWS secrets: %w", err)
		}

		return newS3Writer(ctx, params.AwsS3, opts)
	case params.GcpStorage.BucketName != "":
		if err := params.GcpStorage.LoadSecrets(sa); err != nil {
			return nil, fmt.Errorf("failed to load GCP secrets: %w", err)
		}

		return newGcpWriter(ctx, params.GcpStorage, opts)
	case params.AzureBlob.ContainerName != "":
		if err := params.AzureBlob.LoadSecrets(sa); err != nil {
			return nil, fmt.Errorf("failed to load azure secrets: %w", err)
		}

		return newAzureWriter(ctx, params.AzureBlob, opts)
	default:
		return newLocalWriter(ctx, opts)
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

	return params.BackupXDRParams.RemoveFiles, false
}

func newWriterOpts(
	directory, outputFile string,
	shouldClearTarget, continueBackup bool,
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

	opts = append(opts, ioStorage.WithValidator(asb.NewValidator()))

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

	return blob.NewWriter(ctx, client, a.ContainerName, opts...)
}
