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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const s3DefaultChunkSize = 5 * 1024 * 1024 // 5MB, minimum size of a part

// Writer represents a s3 storage writer.
type Writer struct {
	// Optional parameters.
	options

	client *s3.Client
	// bucketName contains name of the bucket to read from.
	bucketName string
	// prefix contains folder name if we have folders inside the bucket.
	prefix string
	// Sync for running backup to one file.
	called atomic.Bool
}

// NewWriter creates a new writer for S3 storage directory/file writes.
// Must be called with WithDir(path string) or WithFile(path string) - mandatory.
// Can be called with WithRemoveFiles() - optional.
// For S3 client next parameters must be set:
//   - o.UsePathStyle = true
//   - o.BaseEndpoint = &endpoint - if endpoint != ""
func NewWriter(
	ctx context.Context,
	client *s3.Client,
	bucketName string,
	opts ...Opt,
) (*Writer, error) {
	w := &Writer{}

	for _, opt := range opts {
		opt(&w.options)
	}

	if w.path == "" {
		return nil, fmt.Errorf("path is required, use WithDir(path string) or WithFile(path string) to set")
	}

	var prefix string

	if w.isDir {
		w.prefix = w.path
		// Protection from incorrect input.
		if !strings.HasSuffix(w.path, "/") && w.path != "/" && w.path != "" {
			prefix = fmt.Sprintf("%s/", w.path)
		}
	}

	// For s3 we should use empty prefix for root.
	if w.prefix == "/" {
		w.prefix = ""
	}

	// Check if the bucket exists and we have permissions.
	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return nil, fmt.Errorf("bucket %s does not exist or you don't have access: %w", bucketName, err)
	}

	if w.isDir && !w.skipDirCheck {
		// Check if backup dir is empty.
		isEmpty, err := isEmptyDirectory(ctx, client, bucketName, w.prefix)
		if err != nil {
			return nil, fmt.Errorf("failed to check if the directory is empty: %w", err)
		}

		if !isEmpty && !w.isRemovingFiles {
			return nil, fmt.Errorf("backup folder must be empty or set RemoveFiles = true")
		}
	}

	w.client = client
	w.bucketName = bucketName

	if w.isRemovingFiles {
		err = w.RemoveFiles(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to delete files under prefix %s: %w", prefix, err)
		}
	}

	return w, nil
}

// NewWriter returns a new S3 writer to the specified path.
func (w *Writer) NewWriter(ctx context.Context, filename string) (io.WriteCloser, error) {
	// protection for single file backup.
	if !w.isDir {
		if !w.called.CompareAndSwap(false, true) {
			return nil, fmt.Errorf("parallel running for single file is not allowed")
		}
	}

	// If we use backup to single file, we overwrite the file name.
	if !w.isDir {
		filename = w.path
	}

	fullPath := path.Join(w.prefix, filename)

	upload, err := w.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &w.bucketName,
		Key:    &fullPath,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create multipart upload: %w", err)
	}

	return &s3Writer{
		ctx:        ctx,
		uploadID:   upload.UploadId,
		key:        fullPath,
		client:     w.client,
		bucket:     w.bucketName,
		buffer:     new(bytes.Buffer),
		partNumber: 1,
		chunkSize:  s3DefaultChunkSize,
		unbuffered: w.unbuffered,
	}, nil
}

// GetType returns the type of the writer.
func (w *Writer) GetType() string {
	return s3type
}

// s3Writer wrapper for writing files, as S3 in not supporting creation of io.Writer.
type s3Writer struct {
	// ctx is stored internally so that it can be used in io.WriteCloser methods
	ctx            context.Context
	uploadID       *string
	client         *s3.Client
	buffer         *bytes.Buffer
	key            string
	bucket         string
	completedParts []types.CompletedPart
	chunkSize      int
	partNumber     int32
	closed         bool
	unbuffered     bool
}

var _ io.WriteCloser = (*s3Writer)(nil)

func (w *s3Writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, os.ErrClosed
	}

	if w.buffer.Len() >= w.chunkSize {
		err := w.uploadPart()
		if err != nil {
			return 0, fmt.Errorf("failed to upload part: %w", err)
		}
	}

	return w.buffer.Write(p)
}

func (w *s3Writer) uploadPart() error {
	response, err := w.client.UploadPart(w.ctx, &s3.UploadPartInput{
		Body:       bytes.NewReader(w.buffer.Bytes()),
		Bucket:     &w.bucket,
		Key:        &w.key,
		PartNumber: &w.partNumber,
		UploadId:   w.uploadID,
	})

	if err != nil {
		return fmt.Errorf("failed to upload part: %w", err)
	}

	p := w.partNumber
	w.completedParts = append(w.completedParts, types.CompletedPart{
		PartNumber: &p,
		ETag:       response.ETag,
	})

	w.partNumber++
	w.buffer.Reset()

	return nil
}

// uploadDirect is used for unbuffered upload.
func (w *s3Writer) uploadDirect(p []byte) error {
	response, err := w.client.UploadPart(context.Background(), &s3.UploadPartInput{
		Body:       bytes.NewReader(p),
		Bucket:     &w.bucket,
		Key:        &w.key,
		PartNumber: &w.partNumber,
		UploadId:   w.uploadID,
	})

	if err != nil {
		return fmt.Errorf("failed to upload part %d: %w", w.partNumber, err)
	}

	pn := w.partNumber
	w.completedParts = append(w.completedParts, types.CompletedPart{
		PartNumber: &pn,
		ETag:       response.ETag,
	})

	w.partNumber++

	return nil
}

func (w *s3Writer) Close() error {
	if w.closed {
		return os.ErrClosed
	}

	// Upload from buffer only if unbuffered = false.
	if w.buffer.Len() > 0 {
		err := w.uploadPart()
		if err != nil {
			return fmt.Errorf("failed to upload part: %w", err)
		}
	}

	_, err := w.client.CompleteMultipartUpload(w.ctx,
		&s3.CompleteMultipartUploadInput{
			Bucket:   &w.bucket,
			UploadId: w.uploadID,
			Key:      &w.key,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: w.completedParts,
			},
		})
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload, %w", err)
	}

	w.closed = true

	return nil
}

func isEmptyDirectory(ctx context.Context, client *s3.Client, bucketName, prefix string) (bool, error) {
	resp, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  &bucketName,
		Prefix:  &prefix,
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return false, fmt.Errorf("failed to list bucket objects: %w", err)
	}

	// Check if it's a single object
	if len(resp.Contents) == 1 && *resp.Contents[0].Key == prefix {
		return false, nil
	}

	return len(resp.Contents) == 0, nil
}

// RemoveFiles removes a backup file or files from directory.
func (w *Writer) RemoveFiles(ctx context.Context) error {
	// Remove file.
	if !w.isDir {
		if _, err := w.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(w.bucketName),
			Key:    aws.String(w.path),
		}); err != nil {
			return fmt.Errorf("failed to delete object %s: %w", w.path, err)
		}

		return nil
	}
	// Remove files from dir.
	var continuationToken *string

	for {
		listResponse, err := w.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(w.bucketName),
			Prefix:            aws.String(w.prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		for _, p := range listResponse.Contents {
			if p.Key == nil || isDirectory(w.prefix, *p.Key) && !w.withNestedDir {
				continue
			}

			// If validator is set, remove only valid files.
			if w.validator != nil {
				if err = w.validator.Run(*p.Key); err != nil {
					continue
				}
			}

			_, err = w.client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: &w.bucketName,
				Key:    p.Key,
			})
			if err != nil {
				return fmt.Errorf("failed to delete object %s: %w", *p.Key, err)
			}
		}

		continuationToken = listResponse.NextContinuationToken
		if continuationToken == nil {
			break
		}
	}

	return nil
}

func isDirectory(prefix, fileName string) bool {
	// If file name ends with / it is 100% dir.
	if strings.HasSuffix(fileName, "/") {
		return true
	}

	// If we look inside some folder.
	if strings.HasPrefix(fileName, prefix) {
		clean := strings.TrimPrefix(fileName, prefix+"/")
		return strings.Contains(clean, "/")
	}
	// All other variants.
	return strings.Contains(fileName, "/")
}
