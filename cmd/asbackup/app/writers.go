package app

import (
	"context"
	"strings"

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/asbackup/models"
	"github.com/aerospike/backup-go/io/aws/s3"
	"github.com/aerospike/backup-go/io/local"
)

func newLocalWriter(s *models.Storage) (backup.Writer, error) {
	var opts []local.Opt

	if s.Directory != "" && s.OutputFile == "" {
		opts = append(opts, local.WithDir(s.Directory))
	}

	if s.OutputFile != "" && s.Directory == "" {
		opts = append(opts, local.WithFile(s.OutputFile))
	}

	if s.RemoveFiles {
		opts = append(opts, local.WithRemoveFiles())
	}

	return local.NewWriter(opts...)
}

func newS3Writer(ctx context.Context, a *models.AwsS3, s *models.Storage) (backup.Writer, error) {
	client, err := newS3Client(ctx, a)
	if err != nil {
		return nil, err
	}

	var (
		opts             []s3.Opt
		bucketName, path string
	)

	if s.Directory != "" && s.OutputFile == "" {
		bucketName, path = getBucketFromPath(s.Directory)
		opts = append(opts, s3.WithDir(path))
	}

	if s.OutputFile != "" && s.Directory == "" {
		bucketName, path = getBucketFromPath(s.OutputFile)
		opts = append(opts, s3.WithFile(path))
	}

	if s.RemoveFiles {
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
