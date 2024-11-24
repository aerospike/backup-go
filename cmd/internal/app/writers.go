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

func getWriter(
	ctx context.Context,
	backupParams *models.Backup,
	commonParams *models.Common,
	awsS3 *models.AwsS3,
	gcpStorage *models.GcpStorage,
	azureBlob *models.AzureBlob,
	secretAgent *backup.SecretAgentConfig,
) (backup.Writer, error) {
	switch {
	case awsS3.Region != "":
		if err := awsS3.LoadSecrets(secretAgent); err != nil {
			return nil, fmt.Errorf("failed to load AWS secrets: %w", err)
		}

		return newS3Writer(ctx, awsS3, backupParams, commonParams)
	case gcpStorage.BucketName != "":
		if err := gcpStorage.LoadSecrets(secretAgent); err != nil {
			return nil, fmt.Errorf("failed to load GCP secrets: %w", err)
		}

		return newGcpWriter(ctx, gcpStorage, backupParams, commonParams)
	case azureBlob.ContainerName != "":
		if err := azureBlob.LoadSecrets(secretAgent); err != nil {
			return nil, fmt.Errorf("failed to load azure secrets: %w", err)
		}

		return newAzureWriter(ctx, azureBlob, backupParams, commonParams)
	default:
		return newLocalWriter(ctx, backupParams, commonParams)
	}
}

func newLocalWriter(ctx context.Context, b *models.Backup, c *models.Common) (backup.Writer, error) {
	var opts []local.Opt

	if c.Directory != "" && b.OutputFile == "" {
		opts = append(opts, local.WithDir(c.Directory))
	}

	if b.OutputFile != "" && c.Directory == "" {
		opts = append(opts, local.WithFile(b.OutputFile))
	}

	if b.ShouldClearTarget() {
		opts = append(opts, local.WithRemoveFiles())
	}

	if b.Continue != "" {
		opts = append(opts, local.WithSkipDirCheck())
	}

	opts = append(opts, local.WithValidator(asb.NewValidator()))

	return local.NewWriter(ctx, opts...)
}

func newS3Writer(
	ctx context.Context,
	a *models.AwsS3,
	b *models.Backup,
	c *models.Common,
) (backup.Writer, error) {
	client, err := newS3Client(ctx, a)
	if err != nil {
		return nil, err
	}

	opts := make([]s3.Opt, 0)

	if c.Directory != "" && b.OutputFile == "" {
		opts = append(opts, s3.WithDir(c.Directory))
	}

	if b.OutputFile != "" && c.Directory == "" {
		opts = append(opts, s3.WithFile(b.OutputFile))
	}

	if b.ShouldClearTarget() {
		opts = append(opts, s3.WithRemoveFiles())
	}

	if b.Continue != "" {
		opts = append(opts, s3.WithSkipDirCheck())
	}

	opts = append(opts, s3.WithValidator(asb.NewValidator()))

	return s3.NewWriter(ctx, client, a.BucketName, opts...)
}

func newGcpWriter(
	ctx context.Context,
	g *models.GcpStorage,
	b *models.Backup,
	c *models.Common,
) (backup.Writer, error) {
	client, err := newGcpClient(ctx, g)
	if err != nil {
		return nil, err
	}

	opts := make([]storage.Opt, 0)

	if c.Directory != "" && b.OutputFile == "" {
		opts = append(opts, storage.WithDir(c.Directory), storage.WithValidator(asb.NewValidator()))
	}

	if b.OutputFile != "" && c.Directory == "" {
		opts = append(opts, storage.WithFile(b.OutputFile))
	}

	if b.ShouldClearTarget() {
		opts = append(opts, storage.WithRemoveFiles())
	}

	if b.Continue != "" {
		opts = append(opts, storage.WithSkipDirCheck())
	}

	opts = append(opts, storage.WithValidator(asb.NewValidator()))

	return storage.NewWriter(ctx, client, g.BucketName, opts...)
}

func newAzureWriter(
	ctx context.Context,
	a *models.AzureBlob,
	b *models.Backup,
	c *models.Common,
) (backup.Writer, error) {
	client, err := newAzureClient(a)
	if err != nil {
		return nil, err
	}

	opts := make([]blob.Opt, 0)

	if c.Directory != "" && b.OutputFile == "" {
		opts = append(opts, blob.WithDir(c.Directory), blob.WithValidator(asb.NewValidator()))
	}

	if b.OutputFile != "" && c.Directory == "" {
		opts = append(opts, blob.WithFile(b.OutputFile))
	}

	if b.ShouldClearTarget() {
		opts = append(opts, blob.WithRemoveFiles())
	}

	if b.Continue != "" {
		opts = append(opts, blob.WithSkipDirCheck())
	}

	opts = append(opts, blob.WithValidator(asb.NewValidator()))

	return blob.NewWriter(ctx, client, a.ContainerName, opts...)
}
