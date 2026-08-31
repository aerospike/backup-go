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

package streamers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// S3API is the part of the AWS S3 client this package uses. Listing is what it
// leans on: one request describes up to a thousand objects with their keys and
// their sizes, and a delimiter turns the same request into a listing of
// directories, which is how a backup is walked without being downloaded.
type S3API interface {
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options),
	) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options),
	) (*s3.GetObjectOutput, error)
}

// NewS3 creates a streamer over the backup identified by backupID in a bucket.
func NewS3(client S3API, bucket, backupID string, opts ...Option) (*Streamer, error) {
	if client == nil {
		return nil, errors.New("s3 client must not be nil")
	}

	if bucket == "" {
		return nil, errors.New("bucket must not be empty")
	}

	return newStreamer(&s3Store{client: client, bucket: bucket}, backupID, opts...)
}

// s3Store reads a backup out of an S3 bucket, where a path is an object key and
// a directory is a common prefix.
type s3Store struct {
	client S3API
	bucket string
}

// listLevel walks one level of the key tree with a delimiter listing, which
// returns the prefixes of a level as prefixes instead of returning everything
// below them.
func (s *s3Store) listLevel(ctx context.Context, dir string, fn func(levelEntry) error) error {
	prefix := dir + "/"

	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})

	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list %s: %w", prefix, err)
		}

		for _, cp := range page.CommonPrefixes {
			name := strings.Trim(strings.TrimPrefix(aws.ToString(cp.Prefix), prefix), "/")
			if name == "" {
				continue
			}

			if err := fn(levelEntry{Name: name, IsDir: true}); err != nil {
				return stopped(err)
			}
		}

		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)

			// A bucket written through a console holds a zero sized object per
			// directory, which is the directory itself and not a file in it.
			if strings.HasSuffix(key, "/") {
				continue
			}

			entry := levelEntry{
				file: file{Path: key, Size: aws.ToInt64(obj.Size)},
				Name: strings.TrimPrefix(key, prefix),
			}

			if err := fn(entry); err != nil {
				return stopped(err)
			}
		}
	}

	return nil
}

// listFiles pages through everything below dir. Pages are handed over as they
// arrive, so a caller that has seen enough stops the listing instead of paying
// for the rest of it.
func (s *s3Store) listFiles(ctx context.Context, dir string, fn func(file) error) error {
	prefix := dir + "/"

	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})

	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list %s: %w", prefix, err)
		}

		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)

			// A bucket written through a console holds a zero sized object per
			// directory, which is not a file of the backup.
			if strings.HasSuffix(key, "/") {
				continue
			}

			if err := fn(file{Path: key, Size: aws.ToInt64(obj.Size)}); err != nil {
				return stopped(err)
			}
		}
	}

	return nil
}

// open downloads one object.
func (s *s3Store) open(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})

	switch {
	case isNotFound(err):
		return nil, fmt.Errorf("%w: %s", ErrSegmentMissing, key)
	case err != nil:
		return nil, fmt.Errorf("get object %s: %w", key, err)
	}

	return out.Body, nil
}

// isNotFound reports whether an S3 error means the object is not there, as
// opposed to the request having failed.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}

	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}

	switch apiErr.ErrorCode() {
	case "NotFound", "404", "NoSuchKey":
		return true
	default:
		return false
	}
}
