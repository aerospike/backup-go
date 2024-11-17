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
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const s3type = "s3"

type validator interface {
	Run(fileName string) error
}

// Reader represents S3 storage reader.
type Reader struct {
	// Optional parameters.
	options

	client *s3.Client
	// bucketName contains name of the bucket to read from.
	bucketName string
	// prefix contains folder name if we have folders inside the bucket.
	prefix string
}

// NewReader returns new S3 storage reader.
// Must be called with WithDir(path string) or WithFile(path string) - mandatory.
// Can be called with WithValidator(v validator) - optional.
// For S3 client next parameters must be set:
//   - o.UsePathStyle = true
//   - o.BaseEndpoint = &endpoint - if endpoint != ""
func NewReader(
	ctx context.Context,
	client *s3.Client,
	bucketName string,
	opts ...Opt,
) (*Reader, error) {
	r := &Reader{}

	for _, opt := range opts {
		opt(&r.options)
	}

	if r.path == "" {
		return nil, fmt.Errorf("path is required, use WithDir(path string) or WithFile(path string) to set")
	}

	if r.isDir {
		r.prefix = r.path
		// Protection from incorrect input.
		if !strings.HasSuffix(r.path, "/") && r.path != "/" && r.path != "" {
			r.prefix = fmt.Sprintf("%s/", r.path)
		}
	}

	// Check if the bucket exists and we have permissions.
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	}); err != nil {
		return nil, fmt.Errorf("bucket %s does not exist or you don't have access: %w", bucketName, err)
	}

	// S3 storage can read/write to "/" prefix, so we should replace it with "".
	if r.prefix == "/" {
		r.prefix = ""
	}

	r.client = client
	r.bucketName = bucketName

	return r, nil
}

// StreamFiles read files form s3 and send io.Readers to `readersCh` communication
// chan for lazy loading.
// In case of error we send error to `errorsCh` channel.
func (r *Reader) StreamFiles(
	ctx context.Context, readersCh chan<- io.ReadCloser, errorsCh chan<- error,
) {
	// If it is a folder, open and return.
	if r.isDir {
		err := r.checkRestoreDirectory(ctx)
		if err != nil {
			errorsCh <- err
			return
		}

		r.streamDirectory(ctx, readersCh, errorsCh)

		return
	}

	// If not a folder, only file.
	r.StreamFile(ctx, r.path, readersCh, errorsCh)
}

func (r *Reader) streamDirectory(
	ctx context.Context, readersCh chan<- io.ReadCloser, errorsCh chan<- error,
) {
	defer close(readersCh)

	var continuationToken *string

	for {
		listResponse, err := r.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &r.bucketName,
			Prefix:            &r.prefix,
			ContinuationToken: continuationToken,
			StartAfter:        &r.startAfter,
		})

		if err != nil {
			errorsCh <- fmt.Errorf("failed to list objects: %w", err)
			return
		}

		for _, p := range listResponse.Contents {
			if p.Key == nil || isDirectory(r.prefix, *p.Key) && !r.withNestedDir {
				continue
			}

			// Skip not valid files if validator is set.
			if r.validator != nil {
				if err = r.validator.Run(*p.Key); err != nil {
					// Since we are passing invalid files, we don't need to handle this
					// error and write a test for it. Maybe we should log this information
					// for the user, so they know what is going on.
					continue
				}
			}

			var object *s3.GetObjectOutput

			object, err = r.client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: &r.bucketName,
				Key:    p.Key,
			})
			if err != nil {
				// We check *p.Key == nil in the beginning.
				errorsCh <- fmt.Errorf("failed to create reader from directory %s: %w", *p.Key, err)
				return
			}

			readersCh <- object.Body
		}

		continuationToken = listResponse.NextContinuationToken
		if continuationToken == nil {
			break
		}
	}
}

// StreamFile opens single file from s3 and sends io.Readers to the `readersCh`
// In case of an error, it is sent to the `errorsCh` channel.
func (r *Reader) StreamFile(
	ctx context.Context, filename string, readersCh chan<- io.ReadCloser, errorsCh chan<- error) {
	defer close(readersCh)

	if r.isDir {
		filename = filepath.Join(r.path, filename)
	}

	object, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &r.bucketName,
		Key:    &filename,
	})
	if err != nil {
		errorsCh <- fmt.Errorf("failed to create reader from file %s: %w", filename, err)
		return
	}

	readersCh <- object.Body
}

// GetType return `s3type` type of storage. Used in logging.
func (r *Reader) GetType() string {
	return s3type
}

// checkRestoreDirectory checks that the restore directory contains any file.
func (r *Reader) checkRestoreDirectory(ctx context.Context) error {
	var continuationToken *string

	for {
		listResponse, err := r.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &r.bucketName,
			Prefix:            &r.prefix,
			ContinuationToken: continuationToken,
			StartAfter:        &r.startAfter,
		})

		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		for _, p := range listResponse.Contents {
			if p.Key == nil || isDirectory(r.prefix, *p.Key) && !r.withNestedDir {
				continue
			}

			switch {
			case r.validator != nil:
				// If we found a valid file, return.
				if err = r.validator.Run(*p.Key); err == nil {
					return nil
				}
			default:
				// If we found anything, then folder is not empty.
				if p.Key != nil {
					return nil
				}
			}
		}

		continuationToken = listResponse.NextContinuationToken
		if continuationToken == nil {
			break
		}
	}

	return fmt.Errorf("%s is empty", r.prefix)
}
