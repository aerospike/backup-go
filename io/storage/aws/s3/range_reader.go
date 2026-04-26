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
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// downloadReader streams S3 objects for use with [common.RetryableReader].
//
// Reads that start at offset zero use [transfermanager.Client.GetObject] with
// GetObjectRanges so large objects are fetched via parallel ranged GETs. When
// [common.RetryableReader] reopens the stream after a partial read (non-zero
// offset), this type uses a single ranged [Client.GetObject] so the client does
// not re-download from the beginning of the object.
type downloadReader struct {
	client Client
	tm     *transfermanager.Client
	bucket *string
	key    *string
	etag   *string
	size   int64
	// partBodyMaxRetries controls transfer-manager retries for failed part body reads.
	partBodyMaxRetries int
}

// newDownloadReader performs a HeadObject to capture size and ETag, then serves
// reads through the transfer manager or direct GetObject as described for
// [downloadReader].
func newDownloadReader(
	ctx context.Context,
	client Client,
	tm *transfermanager.Client,
	bucket, key *string,
	partBodyMaxRetries int,
) (*downloadReader, error) {
	if key == nil {
		return nil, fmt.Errorf("key is nil")
	}

	if tm == nil {
		return nil, fmt.Errorf("transfer manager client is nil")
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

	return &downloadReader{
		client:             client,
		tm:                 tm,
		bucket:             bucket,
		key:                key,
		etag:               head.ETag,
		size:               size,
		partBodyMaxRetries: partBodyMaxRetries,
	}, nil
}

// OpenRange opens the object. offset 0 and count 0 mean the remainder of the
// object (full object when starting at the beginning).
func (r *downloadReader) OpenRange(ctx context.Context, offset, count int64) (io.ReadCloser, error) {
	if offset == 0 && count == 0 {
		out, err := r.tm.GetObject(ctx, &transfermanager.GetObjectInput{
			Bucket:  r.bucket,
			Key:     r.key,
			IfMatch: r.etag,
		}, func(o *transfermanager.Options) {
			o.PartBodyMaxRetries = r.partBodyMaxRetries
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get object %s: %w", *r.key, err)
		}

		return readerToReadCloser(out.Body), nil
	}

	// Ranged read without transfer manager: checksum mode is not set on range GETs
	// (see https://github.com/aws/aws-sdk-java-v2/issues/5421).
	resp, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:  r.bucket,
		Key:     r.key,
		Range:   getRangeHeader(offset, count),
		IfMatch: r.etag,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s: %w", *r.key, err)
	}

	return resp.Body, nil
}

// GetSize returns file size.
func (r *downloadReader) GetSize() int64 {
	return r.size
}

// GetInfo returns file info for logging.
func (r *downloadReader) GetInfo() string {
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

func readerToReadCloser(body io.Reader) io.ReadCloser {
	if body == nil {
		return nil
	}

	if rc, ok := body.(io.ReadCloser); ok {
		return rc
	}

	return io.NopCloser(body)
}
