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
	"log/slog"
	"path/filepath"
	"strings"
	"sync/atomic"

	"cloud.google.com/go/storage"
	errors2 "github.com/aerospike/backup-go/io/storage/errors"
	"github.com/aerospike/backup-go/io/storage/internal"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aerospike/backup-go/models"
	"google.golang.org/api/iterator"
)

const gcpStorageType = "gcp-storage"

// Reader represents GCP storage reader.
type Reader struct {
	// Optional parameters.
	options.Options

	// bucketHandle contains storage bucket handler for performing reading and writing operations.
	bucketHandle *storage.BucketHandle

	// bucketName contains name of the bucket to read from.
	bucketName string

	// objectsToStream is used to predefine a list of objects that must be read from storage.
	// If objectsToStream is not set, we iterate through objects in storage and load them.
	// If set, we load objects from this slice directly.
	objectsToStream []string

	// total size of all objects in a path.
	totalSize atomic.Int64
	// total number of objects in a path.
	totalNumber atomic.Int64

	// If `skipPrefix` was set on the `StreamFiles` function, skipped file names will be stored here.
	skipped *internal.SkippedFiles
}

// NewReader returns new GCP storage directory/file reader.
// Must be called with WithDir(path string) or WithFile(path string) - mandatory.
// Can be called with WithValidator(v validator) - optional.
func NewReader(
	ctx context.Context,
	client *storage.Client,
	bucketName string,
	opts ...options.Opt,
) (*Reader, error) {
	r := &Reader{}

	for _, opt := range opts {
		opt(&r.Options)
	}

	if len(r.PathList) == 0 {
		return nil, fmt.Errorf("path is required, use WithDir(path string) or WithFile(path string) to set")
	}

	bucket := client.Bucket(bucketName)
	// Check if bucket exists, to avoid errors.
	_, err := bucket.Attrs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket %s attributes: %w", bucketName, err)
	}

	r.bucketHandle = bucket
	r.bucketName = bucketName

	if r.IsDir {
		if !r.SkipDirCheck {
			if err = r.checkRestoreDirectory(ctx, r.PathList[0]); err != nil {
				return nil, fmt.Errorf("%w: %w", errors2.ErrEmptyStorage, err)
			}
		}

		// Presort files if needed.
		if r.SortFiles && len(r.PathList) == 1 {
			if err := internal.PreSort(ctx, r, r.PathList[0]); err != nil {
				return nil, fmt.Errorf("failed to pre sort: %w", err)
			}
		}
	}
	// We "lazy" calculate total size of all files in a path for estimates calculations.
	go r.calculateTotalSize(ctx)

	return r, nil
}

// StreamFiles streams file/directory form GCP cloud storage to `readersCh`.
// If an error occurs, it will be sent to `errorsCh.`
func (r *Reader) StreamFiles(
	ctx context.Context, readersCh chan<- models.File, errorsCh chan<- error, skipPrefixes []string,
) {
	defer close(readersCh)

	// If objects were preloaded, we stream them.
	if len(r.objectsToStream) > 0 {
		r.streamSetObjects(ctx, readersCh, errorsCh)
		return
	}
	// Init file skipper when skipPrefix is set.
	if len(skipPrefixes) > 0 {
		r.skipped = internal.NewSkippedFiles(skipPrefixes)
	}

	for _, path := range r.PathList {
		// If it is a folder, open and return.
		switch r.IsDir {
		case true:
			path = internal.CleanPath(path, false)
			if !r.SkipDirCheck {
				err := r.checkRestoreDirectory(ctx, path)
				if err != nil {
					internal.ErrToChan(ctx, errorsCh, err)
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

func (r *Reader) streamDirectory(
	ctx context.Context, path string, readersCh chan<- models.File, errorsCh chan<- error,
) {
	it := r.bucketHandle.Objects(ctx, &storage.Query{
		Prefix:      path,
		StartOffset: r.StartAfter,
	})

	for {
		// Iterate over bucket until we're done.
		objAttrs, err := it.Next()
		if err != nil {
			if !errors.Is(err, iterator.Done) {
				internal.ErrToChan(ctx, errorsCh, fmt.Errorf("failed to read object attributes from bucket %s: %w",
					r.bucketName, err))
			}
			// If the previous call to Next returned an error other than iterator.Done, all
			// subsequent calls will return the same error. To continue iteration, a new
			// `ObjectIterator` must be created.
			break
		}

		// Skip files in folders.
		if r.shouldSkip(path, objAttrs) {
			continue
		}

		// Skip not valid files if validator is set.
		if r.Validator != nil {
			if err = r.Validator.Run(objAttrs.Name); err != nil {
				// Since we are passing invalid files, we don't need to handle this
				// error and write a test for it. Maybe we should log this information
				// for the user so they know what is going on.
				continue
			}
		}

		// If skipPrefix is set we save skipped filepath and continue.
		if r.skipped.Skip(objAttrs.Name) {
			continue
		}

		r.openObject(ctx, objAttrs.Name, readersCh, errorsCh, true)
	}
}

// openObject creates object readers and sends them to the readersCh.
func (r *Reader) openObject(
	ctx context.Context,
	path string,
	readersCh chan<- models.File,
	errorsCh chan<- error,
	skipNotFound bool,
) {
	reader, err := r.bucketHandle.Object(path).NewReader(ctx)
	if err != nil {
		// Skip 404 not found error.
		if errors.Is(err, storage.ErrObjectNotExist) && skipNotFound {
			return
		}

		internal.ErrToChan(ctx, errorsCh, fmt.Errorf("failed to open directory file %s: %w", path, err))

		return
	}

	if reader != nil {
		readersCh <- models.File{Reader: reader, Name: filepath.Base(path)}
	}
}

// StreamFile opens a single file from GCP cloud storage and sends io.Readers to the `readersCh`
// In case of an error, it is sent to the `errorsCh` channel.
func (r *Reader) StreamFile(
	ctx context.Context, filename string, readersCh chan<- models.File, errorsCh chan<- error) {
	r.openObject(ctx, filename, readersCh, errorsCh, false)
}

// GetType returns the `gcpStorageType` type of storage. Used in logging.
func (r *Reader) GetType() string {
	return gcpStorageType
}

// checkRestoreDirectory checks that the restore directory contains any file.
func (r *Reader) checkRestoreDirectory(ctx context.Context, path string) error {
	it := r.bucketHandle.Objects(ctx, &storage.Query{
		Prefix:      path,
		StartOffset: r.StartAfter,
	})

	for {
		// Iterate over bucket until we're done.
		objAttrs, err := it.Next()
		if err != nil {
			if !errors.Is(err, iterator.Done) {
				return fmt.Errorf("failed to read object attributes from bucket %s: %w",
					r.bucketName, err)
			}
			// If the previous call to Next returned an error other than iterator.Done, all
			// subsequent calls will return the same error. To continue iteration, a new
			// `ObjectIterator` must be created.
			break
		}

		// Skip files in folders.
		if r.shouldSkip(path, objAttrs) {
			continue
		}

		switch {
		case r.Validator != nil:
			// If we found a valid file, return.
			if err = r.Validator.Run(objAttrs.Name); err == nil {
				return nil
			}
		default:
			// If we found anything, then folder is not empty.
			if objAttrs.Name != "" {
				return nil
			}
		}
	}

	return fmt.Errorf("%s is empty", path)
}

// ListObjects list all objects in the path.
func (r *Reader) ListObjects(ctx context.Context, path string) ([]string, error) {
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	result := make([]string, 0)

	it := r.bucketHandle.Objects(ctx, &storage.Query{
		Prefix:      path,
		StartOffset: r.StartAfter,
	})

	for {
		// Iterate over bucket until we're done.
		objAttrs, err := it.Next()
		if err != nil {
			if !errors.Is(err, iterator.Done) {
				return nil, fmt.Errorf("failed to read object attributes from bucket %s: %w",
					r.bucketName, err)
			}

			break
		}

		// Skip files in folders.
		if r.shouldSkip(path, objAttrs) {
			continue
		}

		if objAttrs.Name != "" {
			if r.Validator != nil {
				if err = r.Validator.Run(objAttrs.Name); err != nil {
					continue
				}
			}

			result = append(result, objAttrs.Name)
		}
	}

	return result, nil
}

// SetObjectsToStream sets the objects to stream.
func (r *Reader) SetObjectsToStream(list []string) {
	r.objectsToStream = list
}

// streamSetObjects streams preloaded objects.
func (r *Reader) streamSetObjects(ctx context.Context, readersCh chan<- models.File, errorsCh chan<- error) {
	for i := range r.objectsToStream {
		r.openObject(ctx, r.objectsToStream[i], readersCh, errorsCh, true)
	}
}

// shouldSkip determines whether the file should be skipped.
func (r *Reader) shouldSkip(path string, attr *storage.ObjectAttrs) bool {
	return (internal.IsDirectory(path, attr.Name) && !r.WithNestedDir) || attr.Size == 0
}

func (r *Reader) calculateTotalSize(ctx context.Context) {
	var (
		totalSize int64
		totalNum  int64
	)

	for _, path := range r.PathList {
		size, num, err := r.calculateTotalSizeForPath(ctx, path)
		if err != nil {
			if r.Logger != nil {
				r.Logger.Warn("failed to calculate stats for path",
					slog.String("path", path),
					slog.Any("error", err),
				)
			}
			// Save -1 to signal restore that calculation failed and no need to wait for estimates.
			r.totalSize.Store(-1)
			r.totalNumber.Store(-1)

			// Skip calculation errors.
			return
		}

		totalSize += size
		totalNum += num
	}

	// set size when everything is ready.
	r.totalSize.Store(totalSize)
	r.totalNumber.Store(totalNum)
}

func (r *Reader) calculateTotalSizeForPath(ctx context.Context, path string) (totalSize, totalNum int64, err error) {
	// if we have file to calculate.
	if !r.IsDir {
		objAttrs, err := r.bucketHandle.Object(path).Attrs(ctx)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to get object attributes for %s: %w", path, err)
		}

		return objAttrs.Size, 1, nil
	}

	it := r.bucketHandle.Objects(ctx, &storage.Query{
		Prefix:      path,
		StartOffset: r.StartAfter,
	})

	for {
		// Iterate over bucket until we're done.
		objAttrs, err := it.Next()
		if err != nil {
			if !errors.Is(err, iterator.Done) {
				return 0, 0, fmt.Errorf("failed to read object attributes from bucket %s: %w",
					r.bucketName, err)
			}

			break
		}

		// Skip files in folders.
		if r.shouldSkip(path, objAttrs) {
			continue
		}

		if r.Validator != nil {
			if err = r.Validator.Run(objAttrs.Name); err == nil {
				totalNum++
				totalSize += objAttrs.Size
			}
		}
	}

	return totalSize, totalNum, nil
}

// GetSize returns the size of asb/asbx file/dir that was initialized.
func (r *Reader) GetSize() int64 {
	return r.totalSize.Load()
}

// GetNumber returns the number of asb/asbx files/dirs that was initialized.
func (r *Reader) GetNumber() int64 {
	return r.totalNumber.Load()
}

// GetSkipped returns a list of file paths that were skipped during the `StreamFlies` with skipPrefix.
func (r *Reader) GetSkipped() []string {
	return r.skipped.GetSkipped()
}
