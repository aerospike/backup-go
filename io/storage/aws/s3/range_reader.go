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

package s3

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// s3Getter is an interface for s3 client. Used for mocking tests.
type s3Getter interface {
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options),
	) (*s3.HeadObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options),
	) (*s3.GetObjectOutput, error)
}

// rangeReader encapsulates getting a file by range and file size logic. To use with retry reader.
type rangeReader struct {
	client s3Getter
	bucket *string
	key    *string
	etag   *string

	size int64
}

// newRangeReader creates a new file reader.
func newRangeReader(ctx context.Context, client s3Getter, bucket, key *string) (*rangeReader, error) {
	if key == nil {
		return nil, fmt.Errorf("key is nil")
	}

	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get head %s: %w", *key, err)
	}

	size := int64(0)
	if head.ContentLength != nil {
		size = *head.ContentLength
	}

	return &rangeReader{
		client: client,
		bucket: bucket,
		key:    key,
		etag:   head.ETag,
		size:   size,
	}, nil
}

// OpenRange opens a file by range.
func (r *rangeReader) OpenRange(ctx context.Context, offset, count int64) (io.ReadCloser, error) {
	resp, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       r.bucket,
		Key:          r.key,
		Range:        getRangeHeader(offset, count),
		IfMatch:      r.etag,
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s: %w", *r.key, err)
	}

	return resp.Body, nil
}

// GetSize returns file size.
func (r *rangeReader) GetSize() int64 {
	return r.size
}

// GetInfo returns file info for logging.
func (r *rangeReader) GetInfo() string {
	return fmt.Sprintf("%s:%s", *r.bucket, *r.key)
}

func getRangeHeader(offset, count int64) *string {
	// We read from the current offset till the end of the file.
	// Check https://www.rfc-editor.org/rfc/rfc9110.html#name-byte-ranges for more details.
	switch {
	case offset == 0 && count == 0:
		return nil
	case offset > 0 && count == 0:
		return aws.String(fmt.Sprintf("bytes=%d-", offset))
	case offset > 0 && count > 0:
		return aws.String(fmt.Sprintf("bytes=%d-%d", offset, count))
	default:
		return nil
	}
}
