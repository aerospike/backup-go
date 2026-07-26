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

package server

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aerospike/backup-go/pkg/server/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"golang.org/x/sync/errgroup"
)

const (
	metadataFileName = "metadata.json"
	maxMetadataSize  = 16 << 20

	minCitrusleafTS  int64 = 157_766_400
	clockSkewSeconds int64 = 24 * 60 * 60
	citrusleafEpoch  int64 = 1262304000

	// defaultConcurrency is tuned for network-bound S3 fetches, not CPU.
	// Callers fetching large lists should raise it via WithConcurrency.
	defaultConcurrency = 16

	// S3 error.
	s3ErrNotFound  = "NotFound"
	s3Err404       = "404"
	s3ErrNoSuchKey = "NoSuchKey"
)

// ErrMetadataNotFound is returned when metadata.json is not found.
var ErrMetadataNotFound = errors.New("metadata.json not found")

// S3API is an interface for the S3 client.
type S3API interface {
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options),
	) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options),
	) (*s3.GetObjectOutput, error)
}

// Lister provides functionality to list backups from an S3 bucket.
// It supports listing all snapshots in a given prefix, or a single snapshot.
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

// FetchAllMetadata lists and parses all metadata files under the prefix,
// sorted by backup id (Citrusleaf timestamp) ascending.
func (l *Lister) FetchAllMetadata(ctx context.Context) ([]models.Metadata, error) {
	snapshots, err := l.listSnapshotPrefixes(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}

	if len(snapshots) == 0 {
		return nil, nil
	}

	results := make([]models.Metadata, 0)

	var resultsMu sync.Mutex

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(l.concurrency)

	for _, s := range snapshots {
		g.Go(func() error {
			md, err := l.GetMetadata(gctx, s)
			if err != nil {
				if errors.Is(err, ErrMetadataNotFound) {
					// Skip not finished backups.
					return nil
				}

				// cancellation aborts the whole listing. As we skip errors below, we should doublecheck here.
				if gctx.Err() != nil {
					return gctx.Err()
				}

				l.logger.WarnContext(gctx, "fetch metadata failed",
					slog.String("prefix", s), slog.Any("error", err))
				// Skip errors, and log warning.
				return nil
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

// GetMetadata fetches and parses the metadata for a single backup.
func (l *Lister) GetMetadata(ctx context.Context, backupID string) (models.Metadata, error) {
	data, err := l.fetchOne(ctx, backupID)
	if err != nil {
		return models.Metadata{}, fmt.Errorf("failed to get manifest %s: %w", backupID, err)
	}

	var md models.Metadata
	if err := json.Unmarshal(data, &md); err != nil {
		return models.Metadata{}, fmt.Errorf("failed to parse metadata: %w", err)
	}

	return md, nil
}

func (l *Lister) listSnapshotPrefixes(ctx context.Context) ([]string, error) {
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

			name := strings.TrimSuffix(strings.TrimPrefix(*cp.Prefix, l.prefix), "/")

			_, ok := parseCitrusleafTimestamp(name)
			if !ok {
				l.logger.DebugContext(ctx, "skipping non-snapshot prefix", slog.String("prefix", name))
				continue
			}

			out = append(out, strings.TrimSuffix(*cp.Prefix, "/"))
		}
	}

	return out, nil
}

func (l *Lister) fetchOne(ctx context.Context, path string) ([]byte, error) {
	key := path + "/" + metadataFileName

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

// parseCitrusleafTimestamp parses s as a Citrusleaf-epoch timestamp and reports
// whether it falls within the plausible range for a snapshot folder.
func parseCitrusleafTimestamp(s string) (int64, bool) {
	ts, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}

	if ts < minCitrusleafTS || ts > (time.Now().Unix()-citrusleafEpoch)+clockSkewSeconds {
		return 0, false
	}

	return ts, true
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

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case s3ErrNoSuchKey, s3ErrNotFound, s3Err404:
			return true
		}
	}

	return false
}
