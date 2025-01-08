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
	"github.com/aerospike/backup-go/io/aws/s3"
	"github.com/aerospike/backup-go/io/azure/blob"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/encoding/asbx"
	"github.com/aerospike/backup-go/io/gcp/storage"
	"github.com/aerospike/backup-go/io/local"
)

func newReader(
	ctx context.Context,
	params *ASRestoreParams,
	sa *backup.SecretAgentConfig,
) (backup.StreamingReader, error) {
	directory, inputFile := getDirectoryInputFile(params)
	parentDirectory, directoryList := getParentDirectoryList(params)

	switch {
	case params.AwsS3.Region != "":
		if err := params.AwsS3.LoadSecrets(sa); err != nil {
			return nil, fmt.Errorf("failed to load AWS secrets: %w", err)
		}

		return newS3Reader(ctx, params.AwsS3, directory, inputFile, parentDirectory, directoryList, params.isXDR())
	case params.GcpStorage.BucketName != "":
		if err := params.GcpStorage.LoadSecrets(sa); err != nil {
			return nil, fmt.Errorf("failed to load GCP secrets: %w", err)
		}

		return newGcpReader(ctx, params.GcpStorage, directory, inputFile, parentDirectory, directoryList, params.isXDR())
	case params.AzureBlob.ContainerName != "":
		if err := params.AzureBlob.LoadSecrets(sa); err != nil {
			return nil, fmt.Errorf("failed to load azure secrets: %w", err)
		}

		return newAzureReader(ctx, params.AzureBlob, directory, inputFile, parentDirectory, directoryList, params.isXDR())
	default:
		return newLocalReader(directory, inputFile, parentDirectory, directoryList, params.isXDR())
	}
}

func getDirectoryInputFile(params *ASRestoreParams) (directory, inputFile string) {
	if params.RestoreParams != nil {
		return params.CommonParams.Directory, params.RestoreParams.InputFile
	}
	// Xdr backup.
	return params.RestoreXDRParams.Directory, ""
}

func getParentDirectoryList(params *ASRestoreParams) (parentDirectory, directoryList string) {
	if params.RestoreParams != nil {
		return params.CommonParams.Directory, params.RestoreParams.DirectoryList
	}

	return "", ""
}

func newLocalReader(directory, inputFile, parentDirectory, directoryList string, isXDR bool,
) (backup.StreamingReader, error) {
	opts := make([]local.Opt, 0)

	// As we validate this fields in validation function, we can switch here.
	switch {
	case directory != "":
		opts = append(opts, local.WithDir(directory))
		// Append Validator only for directory.
		if isXDR {
			opts = append(opts, local.WithValidator(asbx.NewValidator()), local.WithSorted(local.SortAsc))
		} else {
			opts = append(opts, local.WithValidator(asb.NewValidator()))
		}
	case inputFile != "":
		opts = append(opts, local.WithFile(inputFile))
	case directoryList != "":
		dirList := prepareDirectoryList(parentDirectory, directoryList)
		opts = append(opts, local.WithDirList(dirList))
	}

	return local.NewReader(opts...)
}

//nolint:dupl // This code is not duplicated, it is a different initialization.
func newS3Reader(
	ctx context.Context,
	a *models.AwsS3,
	directory,
	inputFile,
	parentDirectory,
	directoryList string,
	isXDR bool,
) (backup.StreamingReader, error) {
	client, err := newS3Client(ctx, a)
	if err != nil {
		return nil, err
	}

	opts := make([]s3.Opt, 0)

	// As we validate this fields in validation function, we can switch here.
	switch {
	case directory != "":
		opts = append(opts, s3.WithDir(directory))
		// Append Validator only for directory.
		if isXDR {
			opts = append(opts, s3.WithValidator(asbx.NewValidator()), s3.WithSorted(s3.SortAsc))
		} else {
			opts = append(opts, s3.WithValidator(asb.NewValidator()))
		}
	case inputFile != "":
		opts = append(opts, s3.WithFile(inputFile))
	case directoryList != "":
		dirList := prepareDirectoryList(parentDirectory, directoryList)
		opts = append(opts, s3.WithDirList(dirList))
	}

	return s3.NewReader(ctx, client, a.BucketName, opts...)
}

//nolint:dupl // This code is not duplicated, it is a different initialization.
func newGcpReader(
	ctx context.Context,
	g *models.GcpStorage,
	directory,
	inputFile,
	parentDirectory,
	directoryList string,
	isXDR bool,
) (backup.StreamingReader, error) {
	client, err := newGcpClient(ctx, g)
	if err != nil {
		return nil, err
	}

	opts := make([]storage.Opt, 0)

	// As we validate this fields in validation function, we can switch here.
	switch {
	case directory != "":
		opts = append(opts, storage.WithDir(directory))
		// Append Validator only for directory.
		if isXDR {
			opts = append(opts, storage.WithValidator(asbx.NewValidator()), storage.WithSorted(storage.SortAsc))
		} else {
			opts = append(opts, storage.WithValidator(asb.NewValidator()))
		}
	case inputFile != "":
		opts = append(opts, storage.WithFile(inputFile))
	case directoryList != "":
		dirList := prepareDirectoryList(parentDirectory, directoryList)
		opts = append(opts, storage.WithDirList(dirList))
	}

	return storage.NewReader(ctx, client, g.BucketName, opts...)
}

func newAzureReader(
	ctx context.Context,
	a *models.AzureBlob,
	directory,
	inputFile,
	parentDirectory,
	directoryList string,
	isXDR bool,
) (backup.StreamingReader, error) {
	client, err := newAzureClient(a)
	if err != nil {
		return nil, err
	}

	opts := make([]blob.Opt, 0)

	// As we validate this fields in validation function, we can switch here.
	switch {
	case directory != "":
		opts = append(opts, blob.WithDir(directory))
		// Append Validator only for directory.
		if isXDR {
			opts = append(opts, blob.WithValidator(asbx.NewValidator()), blob.WithSorted(blob.SortAsc))
		} else {
			opts = append(opts, blob.WithValidator(asb.NewValidator()))
		}
	case inputFile != "":
		opts = append(opts, blob.WithFile(inputFile))
	case directoryList != "":
		dirList := prepareDirectoryList(parentDirectory, directoryList)
		opts = append(opts, blob.WithDirList(dirList))
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
