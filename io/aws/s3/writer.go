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
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Writer represents the factory for creating objects that can write to S3.
type Writer struct {
	client   *s3.Client
	s3Config *Config
	fileID   *atomic.Uint32 // increments for each new file created
}

// NewWriter returns a new Writer.
func NewWriter(
	ctx context.Context,
	s3Config *Config,
	removeFiles bool,
) (*Writer, error) {
	if s3Config.MinPartSize > s3maxFile {
		return nil, fmt.Errorf("invalid min part size %d, should not exceed %d", s3Config.MinPartSize, s3maxFile)
	}

	client, err := newS3UploadClient(ctx, s3Config)
	if err != nil {
		return nil, err
	}

	isEmpty, err := isEmptyDirectory(ctx, client, s3Config)
	if err != nil {
		return nil, fmt.Errorf("failed to check if the directory is empty: %w", err)
	}

	if !isEmpty {
		if !removeFiles {
			return nil, fmt.Errorf("backup directory is invalid: %s is not empty", s3Config.Prefix)
		}

		err = deleteAllFilesUnderPrefix(ctx, client, s3Config)
		if err != nil {
			return nil, fmt.Errorf("failed to delete files under prefix %s: %w", s3Config.Prefix, err)
		}
	}

	return &Writer{
		client:   client,
		s3Config: s3Config,
		fileID:   &atomic.Uint32{},
	}, nil
}

// NewWriter returns a new S3 writer to the specified path.
func (f *Writer) NewWriter(ctx context.Context, filename string) (io.WriteCloser, error) {
	chunkSize := f.s3Config.MinPartSize
	if chunkSize < s3DefaultChunkSize {
		chunkSize = s3DefaultChunkSize
	}

	fullPath := path.Join(f.s3Config.Prefix, filename)

	upload, err := f.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &f.s3Config.Bucket,
		Key:    &fullPath,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create multipart upload: %w", err)
	}

	return &s3Writer{
		ctx:        ctx,
		uploadID:   upload.UploadId,
		key:        fullPath,
		client:     f.client,
		bucket:     f.s3Config.Bucket,
		buffer:     new(bytes.Buffer),
		partNumber: 1,
		chunkSize:  chunkSize,
	}, nil
}

// GetType returns the type of the writer.
func (f *Writer) GetType() string {
	return s3type
}

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

func (w *s3Writer) Close() error {
	if w.closed {
		return os.ErrClosed
	}

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

func isEmptyDirectory(ctx context.Context, client *s3.Client, s3Config *Config) (bool, error) {
	resp, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  &s3Config.Bucket,
		Prefix:  &s3Config.Prefix,
		MaxKeys: aws.Int32(1),
	})

	if err != nil {
		return false, fmt.Errorf("failed to list objects: %w", err)
	}

	// Check if it's a single object
	if len(resp.Contents) == 1 && *resp.Contents[0].Key == s3Config.Prefix {
		return false, nil
	}

	return len(resp.Contents) == 0, nil
}

func deleteAllFilesUnderPrefix(ctx context.Context, client *s3.Client, s3Config *Config) error {
	fileCh, errCh := streamFilesFromS3(ctx, client, s3Config)

	for {
		select {
		case file, ok := <-fileCh:
			if !ok {
				fileCh = nil // no more files
			} else {
				_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(s3Config.Bucket),
					Key:    aws.String(file),
				})
				if err != nil {
					return err
				}
			}
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
			} else {
				return err
			}
		}

		if fileCh == nil && errCh == nil { // if no more files and no more errors
			break
		}
	}

	return nil
}
