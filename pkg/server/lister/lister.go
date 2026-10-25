// Copyright 2024-2026 Aerospike, Inc.
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

package lister

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"slices"
	"strings"
	"sync"

	"github.com/aerospike/backup-go/errclass"
	"github.com/aerospike/backup-go/pkg/asinfo"
	"github.com/aerospike/backup-go/pkg/server/lister/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"golang.org/x/sync/errgroup"
)

const (
	metadataFileName = "metadata.json"
	maxMetadataSize  = 16 << 20

	// defaultConcurrency is tuned for network-bound S3 fetches, not CPU.
	// Callers fetching large lists should raise it via WithConcurrency.
	defaultConcurrency = 16

	// S3 error codes that mean the object does not exist.
	s3ErrNotFound  = "NotFound"
	s3Err404       = "404"
	s3ErrNoSuchKey = "NoSuchKey"
)

// ErrMetadataNotFound is returned when a backup has no metadata.json.
// It belongs to the [errclass.ErrNotFound] class.
var ErrMetadataNotFound = fmt.Errorf("%w: metadata.json not found", errclass.ErrNotFound)

// ErrInvalidBackupID is returned when GetMetadata is given something that is not
// a backup id, such as an empty string or a storage path.
// It belongs to the [errclass.ErrInvalidConfig] class.
var ErrInvalidBackupID = fmt.Errorf("%w: invalid backup id", errclass.ErrInvalidConfig)

// S3API is an interface for the S3 client.
type S3API interface {
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options),
	) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options),
	) (*s3.GetObjectOutput, error)
}

// Lister reads the metadata of server-integrated backups from an S3 bucket.
// Backups are stored one folder per backup id under the lister's prefix; the
// Lister reads either all of them or one by its id.
// The concurrency level can be configured. The Lister never prints: it returns
// metadata to the caller, who decides how to render or log it.
type Lister struct {
	client      S3API
	logger      *slog.Logger
	bucket      string
	prefix      string
	concurrency int
}

// Option configures a Lister.
type Option func(*Lister)

// WithConcurrency sets the number of concurrent S3 fetches. Ignored if n <= 0.
func WithConcurrency(n int) Option {
	return func(l *Lister) {
		if n > 0 {
			l.concurrency = n
		}
	}
}

// WithLogger sets the logger. Ignored if logger is nil.
func WithLogger(logger *slog.Logger) Option {
	return func(l *Lister) {
		if logger != nil {
			l.logger = logger
		}
	}
}

// NewLister creates a new backup Lister.
func NewLister(client S3API, bucket, prefix string, opts ...Option) *Lister {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	l := &Lister{
		client:      client,
		bucket:      bucket,
		prefix:      prefix,
		concurrency: defaultConcurrency,
		// nil-safe default so the library never panics on a missing logger.
		logger: slog.New(slog.DiscardHandler),
	}

	for _, opt := range opts {
		opt(l)
	}

	return l
}

// FetchAllMetadata reads the metadata of every backup under the prefix, sorted
// by backup id ascending. A backup without metadata has not finished and is
// skipped; so is one whose metadata cannot be read, with a warning logged.
func (l *Lister) FetchAllMetadata(ctx context.Context) ([]models.Metadata, error) {
	ids, err := l.listBackupIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list backups: %w", err)
	}

	if len(ids) == 0 {
		return nil, nil
	}

	results := make([]models.Metadata, 0, len(ids))

	var resultsMu sync.Mutex

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(l.concurrency)

	for _, id := range ids {
		g.Go(func() error {
			md, err := l.GetMetadata(gctx, id)
			if err != nil {
				return l.skipBackup(gctx, id, err)
			}

			resultsMu.Lock()

			results = append(results, md)
			resultsMu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	slices.SortFunc(results, func(a, b models.Metadata) int {
		return cmp.Compare(a.BackupID, b.BackupID)
	})

	return results, nil
}

// skipBackup decides whether failing to read one backup fails the whole
// listing. Only cancellation does: an unfinished backup is skipped silently and
// any other error is logged.
func (l *Lister) skipBackup(ctx context.Context, backupID string, err error) error {
	switch {
	case errors.Is(err, ErrMetadataNotFound):
		return nil
	case ctx.Err() != nil:
		return ctx.Err()
	default:
		l.logger.WarnContext(ctx, "failed to fetch metadata",
			slog.String("backupId", backupID), slog.Any("error", err))

		return nil
	}
}

// GetMetadata reads the metadata of one backup. backupID is the job id the
// backup was started with, not a storage path: it is looked up under the
// lister's prefix. Anything else fails with [ErrInvalidBackupID].
func (l *Lister) GetMetadata(ctx context.Context, backupID string) (models.Metadata, error) {
	// A job id holds no slash, so the key cannot leave the prefix.
	if !asinfo.IsJobID(backupID) {
		return models.Metadata{}, fmt.Errorf("%w: %q", ErrInvalidBackupID, backupID)
	}

	data, err := l.fetchOne(ctx, backupID)
	if err != nil {
		return models.Metadata{}, fmt.Errorf("failed to get metadata of backup %s: %w", backupID, err)
	}

	var md models.Metadata
	if err := json.Unmarshal(data, &md); err != nil {
		return models.Metadata{}, fmt.Errorf("failed to parse metadata of backup %s: %w", backupID, err)
	}

	return md, nil
}

// listBackupIDs returns the ids of the backup folders under the prefix. Any
// other folder is skipped.
func (l *Lister) listBackupIDs(ctx context.Context) ([]string, error) {
	var out []string

	pg := s3.NewListObjectsV2Paginator(l.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(l.bucket),
		Prefix:    aws.String(l.prefix),
		Delimiter: aws.String("/"),
	})

	for pg.HasMorePages() {
		page, err := pg.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to read next page: %w", err)
		}

		for _, cp := range page.CommonPrefixes {
			if cp.Prefix == nil {
				continue
			}

			id := strings.TrimSuffix(strings.TrimPrefix(*cp.Prefix, l.prefix), "/")

			if !asinfo.IsJobID(id) {
				l.logger.DebugContext(ctx, "skipping non-backup prefix", slog.String("prefix", id))
				continue
			}

			out = append(out, id)
		}
	}

	return out, nil
}

// fetchOne downloads the metadata file of one backup.
func (l *Lister) fetchOne(ctx context.Context, backupID string) ([]byte, error) {
	key := objectKey(l.prefix, backupID, metadataFileName)

	out, err := l.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(l.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, ErrMetadataNotFound
		}

		return nil, fmt.Errorf("failed to get %s: %w", key, err)
	}

	defer out.Body.Close()

	data, err := io.ReadAll(io.LimitReader(out.Body, maxMetadataSize))
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", key, err)
	}

	return data, nil
}

func objectKey(elems ...string) string {
	key := path.Join(elems...)
	if key == "." || key == "/" {
		return ""
	}

	return strings.TrimPrefix(key, "/")
}

// isNotFound reports whether err is an S3 "not found" error.
func isNotFound(err error) bool {
	var (
		noSuchKey *types.NoSuchKey
		notFound  *types.NotFound
	)

	if errors.As(err, &noSuchKey) || errors.As(err, &notFound) {
		return true
	}

	if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
		switch apiErr.ErrorCode() {
		case s3ErrNoSuchKey, s3ErrNotFound, s3Err404:
			return true
		}
	}

	return false
}
