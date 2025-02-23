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
	"path"

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/encoding/asbx"
	ioStorage "github.com/aerospike/backup-go/io/storage"
	"github.com/aerospike/backup-go/io/storage/aws/s3"
	"github.com/aerospike/backup-go/io/storage/azure/blob"
	"github.com/aerospike/backup-go/io/storage/gcp/storage"
	"github.com/aerospike/backup-go/io/storage/local"
)

func newReader(
	ctx context.Context,
	params *ASRestoreParams,
	sa *backup.SecretAgentConfig,
	isXdr bool,
) (backup.StreamingReader, error) {
	directory, inputFile := params.CommonParams.Directory, params.RestoreParams.InputFile
	parentDirectory, directoryList := params.RestoreParams.ParentDirectory, params.RestoreParams.DirectoryList

	opts := newReaderOpts(directory, inputFile, parentDirectory, directoryList, isXdr)

	switch {
	case params.AwsS3 != nil && params.AwsS3.Region != "":
		if err := params.AwsS3.LoadSecrets(sa); err != nil {
			return nil, fmt.Errorf("failed to load AWS secrets: %w", err)
		}

		return newS3Reader(ctx, params.AwsS3, opts)
	case params.GcpStorage != nil && params.GcpStorage.BucketName != "":
		if err := params.GcpStorage.LoadSecrets(sa); err != nil {
			return nil, fmt.Errorf("failed to load GCP secrets: %w", err)
		}

		return newGcpReader(ctx, params.GcpStorage, opts)
	case params.AzureBlob != nil && params.AzureBlob.ContainerName != "":
		if err := params.AzureBlob.LoadSecrets(sa); err != nil {
			return nil, fmt.Errorf("failed to load azure secrets: %w", err)
		}

		return newAzureReader(ctx, params.AzureBlob, opts)
	default:
		return newLocalReader(ctx, opts)
	}
}

func newReaderOpts(
	directory,
	inputFile,
	parentDirectory,
	directoryList string,
	isXDR bool,
) []ioStorage.Opt {
	opts := make([]ioStorage.Opt, 0)

	// As we validate this fields in validation function, we can switch here.
	switch {
	case directory != "":
		opts = append(opts, ioStorage.WithDir(directory))
		// Append Validator only for directory.
		if isXDR {
			opts = append(opts, ioStorage.WithValidator(asbx.NewValidator()), ioStorage.WithSorting())
		} else {
			opts = append(opts, ioStorage.WithValidator(asb.NewValidator()))
		}
	case inputFile != "":
		opts = append(opts, ioStorage.WithFile(inputFile))
	case directoryList != "":
		dirList := prepareDirectoryList(parentDirectory, directoryList)
		opts = append(opts, ioStorage.WithDirList(dirList))
	}

	return opts
}

func newLocalReader(
	ctx context.Context,
	opts []ioStorage.Opt,
) (backup.StreamingReader, error) {
	return local.NewReader(ctx, opts...)
}

func newS3Reader(
	ctx context.Context,
	a *models.AwsS3,
	opts []ioStorage.Opt,
) (backup.StreamingReader, error) {
	client, err := newS3Client(ctx, a)
	if err != nil {
		return nil, err
	}

	return s3.NewReader(ctx, client, a.BucketName, opts...)
}

func newGcpReader(
	ctx context.Context,
	g *models.GcpStorage,
	opts []ioStorage.Opt,
) (backup.StreamingReader, error) {
	client, err := newGcpClient(ctx, g)
	if err != nil {
		return nil, err
	}

	return storage.NewReader(ctx, client, g.BucketName, opts...)
}

func newAzureReader(
	ctx context.Context,
	a *models.AzureBlob,
	opts []ioStorage.Opt,
) (backup.StreamingReader, error) {
	client, err := newAzureClient(a)
	if err != nil {
		return nil, err
	}

	return blob.NewReader(ctx, client, a.ContainerName, opts...)
}

// prepareDirectoryList parses command line parameters and return slice of strings.
func prepareDirectoryList(parentDir, dirList string) []string {
	result := splitByComma(dirList)
	if parentDir != "" {
		for i := range result {
			result[i] = path.Join(parentDir, result[i])
		}
	}

	return result
}
