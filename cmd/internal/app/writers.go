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
	"github.com/aerospike/backup-go/io/local"
)

func newLocalWriter(b *models.Backup) (backup.Writer, error) {
	var opts []local.Opt

	if b.Directory != "" && b.File == "" {
		opts = append(opts, local.WithDir(b.Directory))
	}

	if b.File != "" && b.Directory == "" {
		opts = append(opts, local.WithFile(b.File))
	}

	if b.RemoveFiles {
		opts = append(opts, local.WithRemoveFiles())
	}

	return local.NewWriter(opts...)
}

func newS3Writer(ctx context.Context, a *models.AwsS3, b *models.Backup) (backup.Writer, error) {
	client, err := newS3Client(ctx, a)
	if err != nil {
		return nil, err
	}

	var (
		opts             []s3.Opt
		bucketName, path string
	)

	if b.Directory != "" && b.File == "" {
		bucketName, path = getBucketFromPath(b.Directory)
		opts = append(opts, s3.WithDir(path))
	}

	if b.File != "" && b.Directory == "" {
		bucketName, path = getBucketFromPath(b.File)
		opts = append(opts, s3.WithFile(path))
	}

	if b.RemoveFiles {
		opts = append(opts, s3.WithRemoveFiles())
	}

	return s3.NewWriter(ctx, client, bucketName, opts...)
}

func newGcpWriter() (backup.Writer, error) {
	return nil, nil
}

func newAzureWriter() (backup.Writer, error) {
	return nil, nil
}

// getBucketFromPath returns the first part of path as bucket name.
// E.g.: some/folder/path - returns `some` as bucket name and `folder/path` as cleanPath
func getBucketFromPath(path string) (bucket, cleanPath string) {
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return "", path
	}

	cleanPath = strings.Join(parts[1:], "/")

	return parts[0], cleanPath
}
