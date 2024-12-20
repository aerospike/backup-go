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
	"github.com/aerospike/backup-go/io/gcp/storage"
	"github.com/aerospike/backup-go/io/local"
)

func getReader(
	ctx context.Context,
	restoreParams *models.Restore,
	commonParams *models.Common,
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
	backupParams *models.Backup,
	secretAgent *backup.SecretAgentConfig,
) (backup.StreamingReader, error) {
	switch {
	case awsS3.Region != "":
		if err := awsS3.LoadSecrets(secretAgent); err != nil {
			return nil, fmt.Errorf("failed to load AWS secrets: %w", err)
		}

		return newS3Reader(ctx, awsS3, restoreParams, commonParams, backupParams)
	case gcpStorage.BucketName != "":
		if err := gcpStorage.LoadSecrets(secretAgent); err != nil {
			return nil, fmt.Errorf("failed to load GCP secrets: %w", err)
		}

		return newGcpReader(ctx, gcpStorage, restoreParams, commonParams, backupParams)
	case azureBlob.ContainerName != "":
		if err := azureBlob.LoadSecrets(secretAgent); err != nil {
			return nil, fmt.Errorf("failed to load azure secrets: %w", err)
		}

		return newAzureReader(ctx, azureBlob, restoreParams, commonParams, backupParams)
	default:
		return newLocalReader(restoreParams, commonParams, backupParams)
	}
}

func newLocalReader(r *models.Restore, c *models.Common, b *models.Backup) (backup.StreamingReader, error) {
	opts := make([]local.Opt, 0)

	// As we validate this fields in validation function, we can switch here.
	switch {
	case c.Directory != "":
		opts = append(opts, local.WithDir(c.Directory))
		// Append Validator only if backup params are not set.
		// That means we don't need to check that we are saving a state file.
		if b == nil {
			opts = append(opts, local.WithValidator(asb.NewValidator()))
		}
	case r.InputFile != "":
		opts = append(opts, local.WithFile(r.InputFile))
	case r.DirectoryList != "":
		dirList := prepareDirectoryList(r.ParentDirectory, r.DirectoryList)
		opts = append(opts, local.WithDirList(dirList))
	}

	return local.NewReader(opts...)
}

//nolint:dupl // This code is not duplicated, it is a different initialization.
func newS3Reader(
	ctx context.Context,
	a *models.AwsS3,
	r *models.Restore,
	c *models.Common,
	b *models.Backup,
) (backup.StreamingReader, error) {
	client, err := newS3Client(ctx, a)
	if err != nil {
		return nil, err
	}

	opts := make([]s3.Opt, 0)

	// As we validate this fields in validation function, we can switch here.
	switch {
	case c.Directory != "":
		opts = append(opts, s3.WithDir(c.Directory))
		// Append Validator only if backup params are not set.
		// That means we don't need to check that we are saving a state file.
		if b == nil {
			opts = append(opts, s3.WithValidator(asb.NewValidator()))
		}
	case r.InputFile != "":
		opts = append(opts, s3.WithFile(r.InputFile))
	case r.DirectoryList != "":
		dirList := prepareDirectoryList(r.ParentDirectory, r.DirectoryList)
		opts = append(opts, s3.WithDirList(dirList))
	}

	return s3.NewReader(ctx, client, a.BucketName, opts...)
}

//nolint:dupl // This code is not duplicated, it is a different initialization.
func newGcpReader(
	ctx context.Context,
	g *models.GcpStorage,
	r *models.Restore,
	c *models.Common,
	b *models.Backup,
) (backup.StreamingReader, error) {
	client, err := newGcpClient(ctx, g)
	if err != nil {
		return nil, err
	}

	opts := make([]storage.Opt, 0)

	// As we validate this fields in validation function, we can switch here.
	switch {
	case c.Directory != "":
		opts = append(opts, storage.WithDir(c.Directory))
		// Append Validator only if backup params are not set.
		// That means we don't need to check that we are saving a state file.
		if b == nil {
			opts = append(opts, storage.WithValidator(asb.NewValidator()))
		}
	case r.InputFile != "":
		opts = append(opts, storage.WithFile(r.InputFile))
	case r.DirectoryList != "":
		dirList := prepareDirectoryList(r.ParentDirectory, r.DirectoryList)
		opts = append(opts, storage.WithDirList(dirList))
	}

	return storage.NewReader(ctx, client, g.BucketName, opts...)
}

func newAzureReader(
	ctx context.Context,
	a *models.AzureBlob,
	r *models.Restore,
	c *models.Common,
	b *models.Backup,
) (backup.StreamingReader, error) {
	client, err := newAzureClient(a)
	if err != nil {
		return nil, err
	}

	opts := make([]blob.Opt, 0)

	// As we validate this fields in validation function, we can switch here.
	switch {
	case c.Directory != "":
		opts = append(opts, blob.WithDir(c.Directory))
		// Append Validator only if backup params are not set.
		// That means we don't need to check that we are saving a state file.
		if b == nil {
			opts = append(opts, blob.WithValidator(asb.NewValidator()))
		}
	case r.InputFile != "":
		opts = append(opts, blob.WithFile(r.InputFile))
	case r.DirectoryList != "":
		dirList := prepareDirectoryList(r.ParentDirectory, r.DirectoryList)
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
