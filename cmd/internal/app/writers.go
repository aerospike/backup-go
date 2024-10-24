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
	"strings"

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/backup-go/io/aws/s3"
	"github.com/aerospike/backup-go/io/azure/blob"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/gcp/storage"
	"github.com/aerospike/backup-go/io/local"
)

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

	var bucketName, path string

	opts := make([]s3.Opt, 0)

	if c.Directory != "" && b.OutputFile == "" {
		bucketName, path = getBucketFromPath(c.Directory)
		opts = append(opts, s3.WithDir(path))
	}

	if b.OutputFile != "" && c.Directory == "" {
		bucketName, path = getBucketFromPath(b.OutputFile)
		opts = append(opts, s3.WithFile(path))
	}

	if b.ShouldClearTarget() {
		opts = append(opts, s3.WithRemoveFiles())
	}

	if b.Continue != "" {
		opts = append(opts, s3.WithSkipDirCheck())
	}

	opts = append(opts, s3.WithValidator(asb.NewValidator()))

	return s3.NewWriter(ctx, client, bucketName, opts...)
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

// getBucketFromPath returns the first part of path as bucket name.
// E.g.: some/folder/path - returns `some` as bucket name and `folder/path` as cleanPath
func getBucketFromPath(path string) (bucket, cleanPath string) {
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return path, "/"
	}

	cleanPath = strings.Join(parts[1:], "/")
	if cleanPath == "" {
		cleanPath = "/"
	}

	return parts[0], cleanPath
}
