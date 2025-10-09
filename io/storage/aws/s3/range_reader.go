package s3

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3Getter interface {
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options),
	) (*s3.HeadObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options),
	) (*s3.GetObjectOutput, error)
}

// rangeReader encapsulate getting a file by range and file size logic. To use with retry reader.
type rangeReader struct {
	client s3Getter
	bucket *string
	key    *string
	etag   *string

	size int64
}

// newRangeReader creates a new file reader..
func newRangeReader(ctx context.Context, client s3Getter, bucket, key *string) (*rangeReader, error) {
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
func (r *rangeReader) OpenRange(ctx context.Context, rangeHeader *string) (io.ReadCloser, error) {
	resp, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:  r.bucket,
		Key:     r.key,
		Range:   rangeHeader,
		IfMatch: r.etag,
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
