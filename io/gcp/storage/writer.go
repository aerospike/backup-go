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

const (
	fileType         = "application/octet-stream"
	defaultChunkSize = 5 * 1024 * 1024
)

// Writer represents a GCP storage writer.
type Writer struct {
	// bucketHandle contains storage bucket handler for performing reading and writing operations.
	bucketHandle *storage.BucketHandle
	// prefix contains folder name if we have folders inside the bucket.
	prefix string
}

// NewWriter returns new GCP storage writer.
func NewWriter(
	ctx context.Context,
	client *storage.Client,
	bucketName string,
	folderName string,
	removeFiles bool,
) (*Writer, error) {
	prefix := folderName
	// Protection from incorrect input.
	if !strings.HasSuffix(folderName, "/") && folderName != "/" && folderName != "" {
		prefix = fmt.Sprintf("%s/", folderName)
	}

	bucket := client.Bucket(bucketName)
	// Check if bucket exists, to avoid errors.
	_, err := bucket.Attrs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket attr:%s:  %v", bucketName, err)
	}

	// Check if backup dir is empty.
	isEmpty, err := isEmptyDirectory(ctx, bucket, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to check if bucket is empty: %w", err)
	}

	if !isEmpty && !removeFiles {
		return nil, fmt.Errorf("backup folder must be empty or set removeFiles = true")
	}

	// As we accept only empty dir or dir with files for removing. We can remove them even in an empty bucket.
	if err = removeFilesFromFolder(ctx, bucket, prefix, bucketName); err != nil {
		return nil, fmt.Errorf("failed to remove files from folder: %w", err)
	}

	return &Writer{
		bucketHandle: bucket,
		prefix:       prefix,
	}, nil
}

// NewWriter testing upload.
func (w *Writer) NewWriter(ctx context.Context, filename string) (io.WriteCloser, error) {
	filename = fmt.Sprintf("%s%s", w.prefix, filename)
	sw := w.bucketHandle.Object(filename).NewWriter(ctx)
	sw.ContentType = fileType
	sw.ChunkSize = defaultChunkSize

	return &gcpWriter{
		sw: sw,
	}, nil
}

// GetType return `gcpStorageType` type of storage. Used in logging.
func (w *Writer) GetType() string {
	return gcpStorageType
}

type gcpWriter struct {
	sw *storage.Writer
}

func (w *gcpWriter) Write(p []byte) (n int, err error) {
	return w.sw.Write(p)
}

func (w *gcpWriter) Close() error {
	return w.sw.Close()
}

func isEmptyDirectory(ctx context.Context, bucketHandle *storage.BucketHandle, prefix string) (bool, error) {
	it := bucketHandle.Objects(ctx, &storage.Query{
		Prefix: prefix,
	})

	var filesCount uint

	for {
		// Iterate over bucket until we're done.
		objAttrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return false, fmt.Errorf("failed to list bucket objects: %v", err)
		}

		// Skip files in folders.
		if isDirectory(prefix, objAttrs.Name) {
			continue
		}

		filesCount++
	}
	// Success.
	if filesCount == 0 {
		return true, nil
	}

	return false, nil
}

func removeFilesFromFolder(ctx context.Context, bucketHandle *storage.BucketHandle, prefix, bucketName string) error {
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
			return fmt.Errorf("failed to read object attr from bucket %s: %w", bucketName, err)
		}

		// Skip files in folders.
		if isDirectory(prefix, objAttrs.Name) {
			continue
		}

		if err = bucketHandle.Object(objAttrs.Name).Delete(ctx); err != nil {
			return fmt.Errorf("failed to delete object %s: %w", objAttrs.Name, err)
		}
	}

	return nil
}
