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
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/aerospike/backup-go/internal/util"
	"github.com/aerospike/backup-go/io/common"
	"github.com/aerospike/backup-go/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsHttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
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

	// bucketName contains the name of the bucket to read from.
	bucketName string

	// objectsToStream is used to predefine a list of objects that must be read from storage.
	// If objectsToStream is not set, we iterate through objects in storage and load them.
	// If set, we load objects from this slice directly.
	objectsToStream []string
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

	if len(r.pathList) == 0 {
		return nil, fmt.Errorf("path is required, use WithDir(path string) or WithFile(path string) to set")
	}

	// Check if the bucket exists and we have permissions.
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	}); err != nil {
		return nil, fmt.Errorf("bucket %s does not exist or you don't have access: %w", bucketName, err)
	}

	r.client = client
	r.bucketName = bucketName

	if r.isDir && !r.skipDirCheck {
		if err := r.checkRestoreDirectory(ctx, r.pathList[0]); err != nil {
			return nil, fmt.Errorf("%w: %w", common.ErrEmptyStorage, err)
		}
	}

	// Presort files if needed.
	if err := r.preSort(ctx); err != nil {
		return nil, fmt.Errorf("failed to pre sort: %w", err)
	}

	return r, nil
}

// StreamFiles read files form s3 and send io.Readers to `readersCh` communication chan for lazy loading.
// In case of error, we send error to `errorsCh` channel.
func (r *Reader) StreamFiles(
	ctx context.Context, readersCh chan<- models.File, errorsCh chan<- error,
) {
	defer close(readersCh)

	// If objects were preloaded, we stream them.
	if len(r.objectsToStream) > 0 {
		r.streamSetObjects(ctx, readersCh, errorsCh)
		return
	}

	for _, path := range r.pathList {
		// If it is a folder, open and return.
		switch r.isDir {
		case true:
			path = cleanPath(path)
			if !r.skipDirCheck {
				err := r.checkRestoreDirectory(ctx, path)
				if err != nil {
					errorsCh <- err
					return
				}
			}

			r.streamDirectory(ctx, path, readersCh, errorsCh)
		case false:
			// If not a folder, only file.
			r.StreamFile(ctx, path, readersCh, errorsCh)
		}
	}
}

// streamDirectory reads directory form s3 and send io.Readers to `readersCh` communication chan for lazy loading.
// In case of error, we send error to `errorsCh` channel.
func (r *Reader) streamDirectory(
	ctx context.Context, path string, readersCh chan<- models.File, errorsCh chan<- error,
) {
	var continuationToken *string

	for {
		listResponse, err := r.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &r.bucketName,
			Prefix:            &path,
			ContinuationToken: continuationToken,
			StartAfter:        &r.startAfter,
		})

		if err != nil {
			errorsCh <- fmt.Errorf("failed to list objects: %w", err)
			return
		}

		for _, p := range listResponse.Contents {
			if r.shouldSkip(path, p.Key) {
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

			r.openObject(ctx, *p.Key, readersCh, errorsCh, true)
		}

		continuationToken = listResponse.NextContinuationToken
		if continuationToken == nil {
			break
		}
	}
}

// openObject creates object readers and sends them readersCh.
func (r *Reader) openObject(
	ctx context.Context,
	path string,
	readersCh chan<- models.File,
	errorsCh chan<- error,
	skipNotFound bool,
) {
	object, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &r.bucketName,
		Key:    &path,
	})

	if err != nil {
		// Skip 404 not found error.
		var opErr *smithy.OperationError
		if errors.As(err, &opErr) {
			var httpErr *awsHttp.ResponseError
			if errors.As(opErr.Err, &httpErr) && httpErr.HTTPStatusCode() == http.StatusNotFound && skipNotFound {
				return
			}
		}

		// We check *p.Key == nil in the beginning.
		errorsCh <- fmt.Errorf("failed to open file %s: %w", path, err)

		return
	}

	if object != nil {
		readersCh <- models.File{Reader: object.Body, Name: filepath.Base(path)}
	}
}

// StreamFile opens single file from s3 and sends io.Readers to the `readersCh`
// In case of an error, it is sent to the `errorsCh` channel.
func (r *Reader) StreamFile(
	ctx context.Context, filename string, readersCh chan<- models.File, errorsCh chan<- error) {
	// This condition will be true, only if we initialized reader for directory and then want to read
	// a specific file. It is used for state file and by asb service. So it must be initialized with only
	// one path.
	if r.isDir {
		if len(r.pathList) != 1 {
			errorsCh <- fmt.Errorf("reader must be initialized with only one path")
			return
		}

		filename = filepath.Join(r.pathList[0], filename)
	}

	r.openObject(ctx, filename, readersCh, errorsCh, false)
}

// GetType return `s3type` type of storage. Used in logging.
func (r *Reader) GetType() string {
	return s3type
}

// checkRestoreDirectory checks that the restore directory contains any file.
func (r *Reader) checkRestoreDirectory(ctx context.Context, path string) error {
	var continuationToken *string

	if path == "/" {
		path = ""
	}

	for {
		listResponse, err := r.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &r.bucketName,
			Prefix:            &path,
			ContinuationToken: continuationToken,
			StartAfter:        &r.startAfter,
		})

		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		for _, p := range listResponse.Contents {
			if r.shouldSkip(path, p.Key) {
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

	return fmt.Errorf("%s is empty", path)
}

// ListObjects list all object in the path.
func (r *Reader) ListObjects(ctx context.Context, path string) ([]string, error) {
	var continuationToken *string

	result := make([]string, 0)

	for {
		listResponse, err := r.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &r.bucketName,
			Prefix:            &path,
			ContinuationToken: continuationToken,
			StartAfter:        &r.startAfter,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, p := range listResponse.Contents {
			if r.shouldSkip(path, p.Key) {
				continue
			}

			if p.Key != nil {
				if r.validator != nil {
					if err = r.validator.Run(*p.Key); err != nil {
						continue
					}
				}

				result = append(result, *p.Key)
			}
		}

		continuationToken = listResponse.NextContinuationToken
		if continuationToken == nil {
			break
		}
	}

	return result, nil
}

// shouldSkip performs check, is we should skip files.
func (r *Reader) shouldSkip(path string, fileName *string) bool {
	return fileName == nil || isDirectory(path, *fileName) && !r.withNestedDir
}

// SetObjectsToStream set objects to stream.
func (r *Reader) SetObjectsToStream(list []string) {
	r.objectsToStream = list
}

// streamSetObjects streams preloaded objects.
func (r *Reader) streamSetObjects(ctx context.Context, readersCh chan<- models.File, errorsCh chan<- error) {
	for i := range r.objectsToStream {
		r.openObject(ctx, r.objectsToStream[i], readersCh, errorsCh, true)
	}
}

// cleanPath is protection from incorrect input.
func cleanPath(path string) string {
	// S3 storage can read/write to "/" prefix, so we should replace it with "".
	if path == "/" {
		return ""
	}

	result := path
	if !strings.HasSuffix(path, "/") && path != "/" && path != "" {
		result = fmt.Sprintf("%s/", path)
	}

	return result
}

// preSort performs files sorting before read.
func (r *Reader) preSort(ctx context.Context) error {
	if !r.sortFiles || len(r.pathList) != 1 {
		return nil
	}

	// List all files first.
	list, err := r.ListObjects(ctx, r.pathList[0])
	if err != nil {
		return err
	}

	// Sort files.
	list, err = util.SortBackupFiles(list)
	if err != nil {
		return err
	}

	// Pass sorted list to reader.
	r.SetObjectsToStream(list)

	return nil
}
