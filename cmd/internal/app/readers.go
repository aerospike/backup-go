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

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/backup-go/io/aws/s3"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/local"
)

func newLocalReader(r *models.Restore, c *models.Common) (backup.StreamingReader, error) {
	var opts []local.Opt

	if c.Directory != "" && r.InputFile == "" {
		opts = append(opts, local.WithDir(c.Directory), local.WithValidator(asb.NewValidator()))
	}

	if r.InputFile != "" && c.Directory == "" {
		opts = append(opts, local.WithFile(r.InputFile))
	}

	return local.NewReader(opts...)
}

func newS3Reader(
	ctx context.Context,
	a *models.AwsS3,
	r *models.Restore,
	c *models.Common,
) (backup.StreamingReader, error) {
	client, err := newS3Client(ctx, a)
	if err != nil {
		return nil, err
	}

	var (
		opts             []s3.Opt
		bucketName, path string
	)

	if c.Directory != "" && r.InputFile == "" {
		bucketName, path = getBucketFromPath(c.Directory)
		opts = append(opts, s3.WithDir(path), s3.WithValidator(asb.NewValidator()))
	}

	if r.InputFile != "" && c.Directory == "" {
		bucketName, path = getBucketFromPath(r.InputFile)
		opts = append(opts, s3.WithFile(path))
	}

	return s3.NewReader(ctx, client, bucketName, opts...)
}

func newGcpReader() (backup.StreamingReader, error) {
	return nil, nil
}

func newAzureReader() (backup.StreamingReader, error) {
	return nil, nil
}
