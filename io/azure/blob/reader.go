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

package blob

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	azErr "github.com/Azure/azure-sdk-for-go-extensions/pkg/errors"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

const azureBlobType = "azure-blob"

type validator interface {
	Run(fileName string) error
}

// Reader represents GCP storage reader.
type Reader struct {
	// Optional parameters.
	options

	client *azblob.Client
	// containerName contains name of the container to read from.
	containerName string
	// prefix contains folder name if we have folders inside the bucket.
	prefix string
}

// NewReader returns new Azure blob directory/file reader.
// Must be called with WithDir(path string) or WithFile(path string) - mandatory.
// Can be called with WithValidator(v validator) - optional.
func NewReader(
	ctx context.Context,
	client *azblob.Client,
	containerName string,
	opts ...Opt,
) (*Reader, error) {
	r := &Reader{
		client: client,
	}

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

	// Check if container exists.
	if _, err := client.ServiceClient().NewContainerClient(containerName).GetProperties(ctx, nil); err != nil {
		return nil, fmt.Errorf("unable to get container properties: %w", err)
	}

	r.containerName = containerName

	return r, nil
}

// StreamFiles streams file/directory form GCP cloud storage to `readersCh`.
// If error occurs, it will be sent to `errorsCh.`
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

	pager := r.client.NewListBlobsFlatPager(r.containerName, &azblob.ListBlobsFlatOptions{
		Prefix: &r.prefix,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			errorsCh <- fmt.Errorf("failed to get next page: %w", err)
		}

		for _, blob := range page.Segment.BlobItems {
			// Skip files in folders.
			if r.shouldSkip(*blob.Name) {
				continue
			}

			// Skip not valid files if validator is set.
			if r.validator != nil {
				if err = r.validator.Run(*blob.Name); err != nil {
					// Since we are passing invalid files, we don't need to handle this
					// error and write a test for it. Maybe we should log this information
					// for the user, so they know what is going on.
					continue
				}
			}

			var resp azblob.DownloadStreamResponse

			resp, err = r.client.DownloadStream(ctx, r.containerName, *blob.Name, nil)
			if err != nil {
				// Skip 404 not found error.
				if azErr.IsNotFoundErr(err) {
					continue
				}

				errorsCh <- fmt.Errorf("failed to create reader from file %s: %w", *blob.Name, err)

				return
			}

			readersCh <- resp.Body
		}
	}
}

// StreamFile opens a single file from GCP cloud storage and sends io.Readers to the `readersCh`
// In case of an error, it is sent to the `errorsCh` channel.
func (r *Reader) StreamFile(
	ctx context.Context, filename string, readersCh chan<- io.ReadCloser, errorsCh chan<- error) {
	defer close(readersCh)

	if r.isDir {
		filename = filepath.Join(r.path, filename)
	}

	resp, err := r.client.DownloadStream(ctx, r.containerName, filename, nil)
	if err != nil {
		errorsCh <- fmt.Errorf("failed to create reader from file %s: %w", filename, err)
		return
	}

	readersCh <- resp.Body
}

// shouldSkip performs check, is we should skip files.
func (r *Reader) shouldSkip(fileName string) bool {
	return (isDirectory(r.prefix, fileName) && !r.withNestedDir) ||
		isSkippedByStartAfter(r.startAfter, fileName)
}

// GetType return `gcpStorageType` type of storage. Used in logging.
func (r *Reader) GetType() string {
	return azureBlobType
}

// checkRestoreDirectory checks that the restore directory contains any file.
func (r *Reader) checkRestoreDirectory(ctx context.Context) error {
	pager := r.client.NewListBlobsFlatPager(r.containerName, &azblob.ListBlobsFlatOptions{
		Prefix: &r.prefix,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to get next page: %w", err)
		}

		for _, blob := range page.Segment.BlobItems {
			// Skip files in folders.
			if r.shouldSkip(*blob.Name) {
				continue
			}

			switch {
			case r.validator != nil:
				// If we found a valid file, return.
				if err = r.validator.Run(*blob.Name); err == nil {
					return nil
				}
			default:
				// If we found anything, then folder is not empty.
				if blob.Name != nil {
					return nil
				}
			}
		}
	}

	return fmt.Errorf("%s is empty", r.prefix)
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

func isSkippedByStartAfter(startAfter, fileName string) bool {
	if startAfter == "" {
		return false
	}

	if fileName <= startAfter {
		return true
	}

	return false
}
