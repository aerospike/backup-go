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

// StreamingReader describes GCP Storage streaming reader.
type StreamingReader struct {
	// bucketHandle contains storage bucket handler for performing reading and writing operations.
	bucketHandle *storage.BucketHandle
	// bucketName contains name of the bucket to read from.
	bucketName string
	// prefix contains folder name if we have folders inside the bucket.
	prefix string
	// validator files validator.
	validator validator
}

// NewStreamingReader returns new GCP storage streaming reader.
func NewStreamingReader(
	ctx context.Context,
	client *storage.Client,
	bucketName string,
	folderName string,
	validator validator,
) (*StreamingReader, error) {
	if validator == nil {
		return nil, fmt.Errorf("validator cannot be nil")
	}

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

	return &StreamingReader{
		bucketHandle: bucket,
		bucketName:   bucketName,
		prefix:       prefix,
		validator:    validator,
	}, nil
}

// StreamFiles streams files form GCP cloud storage to `readersCh`.
// If error occurs, it will be sent to `errorsCh.`
func (r *StreamingReader) StreamFiles(
	ctx context.Context, readersCh chan<- io.ReadCloser, errorsCh chan<- error,
) {
	it := r.bucketHandle.Objects(ctx, &storage.Query{
		Prefix: r.prefix,
	})

	for {
		// Iterate over bucket until we're done.
		objAttrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			errorsCh <- fmt.Errorf("failed to read object attr from bucket %s: %w", r.bucketName, err)
		}

		// Skip files in folders.
		if isDirectory(r.prefix, objAttrs.Name) {
			continue
		}

		// Skip not valid files.
		if err = r.validator.Run(objAttrs.Name); err != nil {
			continue
		}

		// Create readers for files.
		var reader *storage.Reader

		reader, err = r.bucketHandle.Object(objAttrs.Name).NewReader(ctx)
		if err != nil {
			errorsCh <- fmt.Errorf("failerd to create reader from file %s: %w", objAttrs.Name, err)
		}

		readersCh <- reader
	}

	close(readersCh)
}

// OpenFile opens single file from GCP cloud storage and sends io.Readers to the `readersCh`
// In case of an error, it is sent to the `errorsCh` channel.
func (r *StreamingReader) OpenFile(
	ctx context.Context, filename string, readersCh chan<- io.ReadCloser, errorsCh chan<- error) {
	defer close(readersCh)

	if !strings.Contains(filename, "/") {
		filename = fmt.Sprintf("%s%s", r.prefix, filename)
	}

	reader, err := r.bucketHandle.Object(filename).NewReader(ctx)
	if err != nil {
		errorsCh <- fmt.Errorf("failed to open %s: %w", filename, err)
		return
	}

	readersCh <- reader
}

// GetType return `gcpStorageType` type of storage. Used in logging.
func (r *StreamingReader) GetType() string {
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
