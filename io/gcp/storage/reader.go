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

package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

const gcpStorageType = "gcp-storage"

type validator interface {
	Run(fileName string) error
}

// Reader represents GCP storage reader.
type Reader struct {
	// Optional parameters.
	options

	// bucketHandle contains storage bucket handler for performing reading and writing operations.
	bucketHandle *storage.BucketHandle
	// bucketName contains name of the bucket to read from.
	bucketName string
	// prefix contains folder name if we have folders inside the bucket.
	prefix string
}

type options struct {
	// path contains path to file or directory.
	path string
	// isDir flag describes what we have in path, file or directory.
	isDir bool
	// removeFiles flag describes should we remove everything from backup folder or not.
	removeFiles bool
	// validator contains files validator that is applied to files if isDir = true.
	validator validator
}

type Opt func(*options)

// WithDir adds directory to reading/writing files from/to.
func WithDir(path string) Opt {
	return func(r *options) {
		r.path = path
		r.isDir = true
	}
}

// WithFile adds a file path to reading/writing from/to.
func WithFile(path string) Opt {
	return func(r *options) {
		r.path = path
		r.isDir = false
		r.removeFiles = true
	}
}

// WithValidator adds validator to Reader, so files will be validated before reading.
// Is used only for Reader.
func WithValidator(v validator) Opt {
	return func(r *options) {
		r.validator = v
	}
}

// NewReader returns new GCP storage directory/file reader.
// Must be called with WithDir(path string) or WithFile(path string) - mandatory.
// Can be called with WithValidator(v validator) - optional.
func NewReader(
	ctx context.Context,
	client *storage.Client,
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

	bucket := client.Bucket(bucketName)
	// Check if bucket exists, to avoid errors.
	_, err := bucket.Attrs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket attr:%s:  %v", bucketName, err)
	}

	r.bucketHandle = bucket
	r.bucketName = bucketName

	return r, nil
}

// StreamFiles streams file/directory form GCP cloud storage to `readersCh`.
// If error occurs, it will be sent to `errorsCh.`
func (r *Reader) StreamFiles(
	ctx context.Context, readersCh chan<- io.ReadCloser, errorsCh chan<- error,
) {
	// If it is a folder, open and return.
	if r.isDir {
		r.streamDirectory(ctx, readersCh, errorsCh)
		return
	}

	// If not a folder, only file.
	r.streamFile(ctx, r.path, readersCh, errorsCh)
}

func (r *Reader) streamDirectory(
	ctx context.Context, readersCh chan<- io.ReadCloser, errorsCh chan<- error,
) {
	defer close(readersCh)

	it := r.bucketHandle.Objects(ctx, &storage.Query{
		Prefix: r.prefix,
	})

	for {
		// Iterate over bucket until we're done.
		objAttrs, err := it.Next()
		if err != nil {
			if !errors.Is(err, iterator.Done) {
				errorsCh <- fmt.Errorf("failed to read object attr from bucket %s: %w",
					r.bucketName, err)
			}
			// If the previous call to Next returned an error other than iterator.Done, all
			// subsequent calls will return the same error. To continue iteration, a new
			// `ObjectIterator` must be created.
			break
		}

		// Skip files in folders.
		if isDirectory(r.prefix, objAttrs.Name) {
			continue
		}

		// Skip not valid files if validator is set.
		if r.validator != nil {
			if err = r.validator.Run(objAttrs.Name); err != nil {
				// Since we are passing invalid files, we don't need to handle this
				// error and write a test for it. Maybe we should log this information
				// for the user so they know what is going on.
				continue
			}
		}

		// Create readers for files.
		var reader *storage.Reader

		reader, err = r.bucketHandle.Object(objAttrs.Name).NewReader(ctx)
		if err != nil {
			errorsCh <- fmt.Errorf("failed to create reader from file %s: %w", objAttrs.Name, err)
		}

		if reader != nil {
			readersCh <- reader
		}
	}
}

// streamFile opens a single file from GCP cloud storage and sends io.Readers to the `readersCh`
// In case of an error, it is sent to the `errorsCh` channel.
func (r *Reader) streamFile(
	ctx context.Context, filename string, readersCh chan<- io.ReadCloser, errorsCh chan<- error) {
	defer close(readersCh)

	reader, err := r.bucketHandle.Object(filename).NewReader(ctx)
	if err != nil {
		errorsCh <- fmt.Errorf("failed to open %s: %w", filename, err)
		return
	}

	readersCh <- reader
}

// GetType return `gcpStorageType` type of storage. Used in logging.
func (r *Reader) GetType() string {
	return gcpStorageType
}

func isDirectory(prefix, fileName string) bool {
	// If file name ends with / it is 100% dir.
	if strings.HasSuffix(fileName, "/") {
		return true
	}

	// If we look inside some folder.
	if strings.HasPrefix(fileName, prefix) {
		clean := strings.TrimPrefix(fileName, prefix)
		return strings.Contains(clean, "/")
	}
	// All other variants.
	return strings.Contains(fileName, "/")
}
