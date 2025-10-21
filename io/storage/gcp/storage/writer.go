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
	"path"
	"sync/atomic"

	"cloud.google.com/go/storage"
	"github.com/aerospike/backup-go/io/storage/common"
	"github.com/aerospike/backup-go/io/storage/options"
	"google.golang.org/api/iterator"
)

const (
	fileType         = "application/octet-stream"
	defaultChunkSize = 5 * 1024 * 1024
)

// Writer represents a GCP storage writer.
type Writer struct {
	// Optional parameters.
	options.Options
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
	opts ...options.Opt,
) (*Writer, error) {
	w := &Writer{}

	for _, opt := range opts {
		opt(&w.Options)
	}

	if w.ChunkSize < 0 {
		return nil, fmt.Errorf("chunk size must be positive")
	}

	if w.ChunkSize == 0 {
		w.ChunkSize = defaultChunkSize
	}

	if len(w.PathList) != 1 {
		return nil, fmt.Errorf("one path is required, use WithDir(path string) or WithFile(path string) to set")
	}

	if w.IsDir {
		w.prefix = common.CleanPath(w.PathList[0], false)
	}

	bucketHandler := client.Bucket(bucketName)
	// Check if bucketHandler exists, to avoid errors.
	_, err := bucketHandler.Attrs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucketHandler %s attributes: %w", bucketName, err)
	}

	if w.IsDir && !w.SkipDirCheck {
		// Check if backup dir is empty.
		isEmpty, err := isEmptyDirectory(ctx, bucketHandler, w.prefix)
		if err != nil {
			return nil, fmt.Errorf("failed to check if directory is empty: %w", err)
		}

		if !isEmpty && !w.IsRemovingFiles {
			return nil, fmt.Errorf("backup folder must be empty or set RemoveFiles = true")
		}
	}

	w.bucketHandle = bucketHandler

	if w.IsRemovingFiles {
		// As we accept only empty dir or dir with files for removing. We can remove them even in an empty bucketHandler.
		if err = w.RemoveFiles(ctx); err != nil {
			return nil, fmt.Errorf("failed to remove files from folder: %w", err)
		}
	}

	return w, nil
}

// NewWriter returns a new GCP storage writer for the provided path.
// isMeta describe if the file is a metadata file.
func (w *Writer) NewWriter(ctx context.Context, filename string, isMeta bool) (io.WriteCloser, error) {
	// protection for single file backup.
	if !w.IsDir {
		if !isMeta && !w.called.CompareAndSwap(false, true) {
			return nil, fmt.Errorf("parallel running for single file is not allowed")
		}
	}

	var fullPath string

	switch {
	case w.IsDir:
		fullPath = path.Join(w.prefix, filename)
	case isMeta && !w.IsDir:
		// If it is metadata file and we backup to one file.
		fullPath = path.Join(path.Dir(w.PathList[0]), filename)
	default:
		// If we use backup to single file, we overwrite the file name.
		fullPath = path.Join(w.prefix, w.PathList[0])
	}

	sw := w.bucketHandle.Object(fullPath).NewWriter(ctx)
	sw.ContentType = fileType
	sw.ChunkSize = w.ChunkSize
	sw.StorageClass = w.StorageClass

	return sw, nil
}

// RemoveFiles removes a backup file or files from directory.
func (w *Writer) RemoveFiles(ctx context.Context) error {
	return w.Remove(ctx, w.PathList[0])
}

// Remove deletes the file or directory contents specified by path.
func (w *Writer) Remove(ctx context.Context, targetPath string) error {
	// Remove file.
	if !w.IsDir {
		if err := w.bucketHandle.Object(targetPath).Delete(ctx); err != nil {
			return fmt.Errorf("failed to delete object %s: %w", targetPath, err)
		}

		return nil
	}

	prefix := common.CleanPath(targetPath, false)
	// Remove files from dir.
	it := w.bucketHandle.Objects(ctx, &storage.Query{
		Prefix: prefix,
	})

	for {
		// Iterate over bucket until we're done.
		objAttrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return fmt.Errorf("failed to read object attributes from bucket %s: %w", w.bucketName, err)
		}

		// Skip files in folders.
		if common.IsDirectory(prefix, objAttrs.Name) && !w.WithNestedDir {
			continue
		}

		// If validator is set, remove only valid files.
		if w.Validator != nil {
			if err = w.Validator.Run(objAttrs.Name); err != nil {
				continue
			}
		}

		if err = w.bucketHandle.Object(objAttrs.Name).Delete(ctx); err != nil {
			return fmt.Errorf("failed to delete object %s: %w", objAttrs.Name, err)
		}
	}

	return nil
}

// GetType returns the `gcpStorageType` type of storage. Used in logging.
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
		if common.IsDirectory(prefix, objAttrs.Name) {
			continue
		}

		return false, nil
	}

	return true, nil
}
