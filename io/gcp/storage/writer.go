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
	"sync/atomic"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

const (
	fileType         = "application/octet-stream"
	defaultChunkSize = 5 * 1024 * 1024
)

// Writer represents a GCP storage writer.
type Writer struct {
	// Optional parameters.
	options
	// bucketName contains bucket name, is used for logging.
	bucketName string
	// bucketHandle contains storage bucket handler for performing reading and writing operations.
	bucketHandle *storage.BucketHandle
	// prefix contains folder name if we have folders inside the bucket.
	prefix string
	// Sync for running backup to one file.
	called atomic.Bool
}

// NewWriter creates a new writer for GCP storage directory/file writes.
// Must be called with WithDir(path string) or WithFile(path string) - mandatory.
// Can be called with WithRemoveFiles() - optional.
func NewWriter(
	ctx context.Context,
	client *storage.Client,
	bucketName string,
	opts ...Opt,
) (*Writer, error) {
	w := &Writer{}

	for _, opt := range opts {
		opt(&w.options)
	}

	if len(w.pathList) != 1 {
		return nil, fmt.Errorf("one path is required, use WithDir(path string) or WithFile(path string) to set")
	}

	if w.isDir {
		w.prefix = cleanPath(w.pathList[0])
	}

	bucketHandler := client.Bucket(bucketName)
	// Check if bucketHandler exists, to avoid errors.
	_, err := bucketHandler.Attrs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucketHandler %s attr: %w", bucketName, err)
	}

	if w.isDir && !w.skipDirCheck {
		// Check if backup dir is empty.
		isEmpty, err := isEmptyDirectory(ctx, bucketHandler, w.prefix)
		if err != nil {
			return nil, fmt.Errorf("failed to check if directory is empty: %w", err)
		}

		if !isEmpty && !w.isRemovingFiles {
			return nil, fmt.Errorf("backup folder must be empty or set RemoveFiles = true")
		}
	}

	w.bucketHandle = bucketHandler

	if w.isRemovingFiles {
		// As we accept only empty dir or dir with files for removing. We can remove them even in an empty bucketHandler.
		if err = w.RemoveFiles(ctx); err != nil {
			return nil, fmt.Errorf("failed to remove files from folder: %w", err)
		}
	}

	return w, nil
}

// NewWriter returns a new GCP storage writer to the specified path.
func (w *Writer) NewWriter(ctx context.Context, filename string) (io.WriteCloser, error) {
	// protection for single file backup.
	if !w.isDir {
		if !w.called.CompareAndSwap(false, true) {
			return nil, fmt.Errorf("parallel running for single file is not allowed")
		}
		// If we use backup to single file, we overwrite the file name.
		filename = w.pathList[0]
	}

	filename = fmt.Sprintf("%s%s", w.prefix, filename)
	sw := w.bucketHandle.Object(filename).NewWriter(ctx)
	sw.ContentType = fileType
	sw.ChunkSize = defaultChunkSize

	return sw, nil
}

// RemoveFiles removes a backup file or files from directory.
func (w *Writer) RemoveFiles(
	ctx context.Context,
) error {
	// Remove file.
	if !w.isDir {
		if err := w.bucketHandle.Object(w.pathList[0]).Delete(ctx); err != nil {
			return fmt.Errorf("failed to delete object %s: %w", w.pathList[0], err)
		}

		return nil
	}
	// Remove files from dir.
	it := w.bucketHandle.Objects(ctx, &storage.Query{
		Prefix: w.pathList[0],
	})

	for {
		// Iterate over bucket until we're done.
		objAttrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return fmt.Errorf("failed to read object attr from bucket %s: %w", w.bucketName, err)
		}

		// Skip files in folders.
		if isDirectory(w.pathList[0], objAttrs.Name) && !w.withNestedDir {
			continue
		}

		// If validator is set, remove only valid files.
		if w.validator != nil {
			if err = w.validator.Run(objAttrs.Name); err != nil {
				continue
			}
		}

		if err = w.bucketHandle.Object(objAttrs.Name).Delete(ctx); err != nil {
			return fmt.Errorf("failed to delete object %s: %w", objAttrs.Name, err)
		}
	}

	return nil
}

// GetType return `gcpStorageType` type of storage. Used in logging.
func (w *Writer) GetType() string {
	return gcpStorageType
}

func isEmptyDirectory(ctx context.Context, bucketHandle *storage.BucketHandle, prefix string) (bool, error) {
	it := bucketHandle.Objects(ctx, &storage.Query{
		Prefix: prefix,
	})

	for {
		// Iterate over bucket until we're done.
		objAttrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return false, fmt.Errorf("failed to list bucket objects: %w", err)
		}

		// Skip files in folders.
		if isDirectory(prefix, objAttrs.Name) {
			continue
		}

		return false, nil
	}

	return true, nil
}
