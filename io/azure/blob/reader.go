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
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aerospike/backup-go/internal/util"
	"github.com/aerospike/backup-go/io/storage"
	"github.com/aerospike/backup-go/models"
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

	// objectsToStream is used to predefine a list of objects that must be read from storage.
	// If objectsToStream is not set, we iterate through objects in storage and load them.
	// If set, we load objects from this slice directly.
	objectsToStream []string
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

	if len(r.pathList) == 0 {
		return nil, fmt.Errorf("path is required, use WithDir(path string) or WithFile(path string) to set")
	}

	// Check if container exists.
	if _, err := client.ServiceClient().NewContainerClient(containerName).GetProperties(ctx, nil); err != nil {
		return nil, fmt.Errorf("unable to get container properties: %w", err)
	}

	r.containerName = containerName

	if r.isDir && !r.skipDirCheck {
		if err := r.checkRestoreDirectory(ctx, r.pathList[0]); err != nil {
			return nil, fmt.Errorf("%w: %w", storage.ErrEmptyStorage, err)
		}
	}

	// Presort files if needed.
	if err := r.preSort(ctx); err != nil {
		return nil, fmt.Errorf("failed to pre sort: %w", err)
	}

	return r, nil
}

// StreamFiles streams file/directory form GCP cloud storage to `readersCh`.
// If error occurs, it will be sent to `errorsCh.`
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

func (r *Reader) streamDirectory(
	ctx context.Context, path string, readersCh chan<- models.File, errorsCh chan<- error,
) {
	pager := r.client.NewListBlobsFlatPager(r.containerName, &azblob.ListBlobsFlatOptions{
		Prefix: &path,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			errorsCh <- fmt.Errorf("failed to get next page: %w", err)
		}

		for _, blob := range page.Segment.BlobItems {
			// Skip files in folders.
			if r.shouldSkip(path, *blob.Name) {
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

			r.openObject(ctx, *blob.Name, readersCh, errorsCh, true)
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
	resp, err := r.client.DownloadStream(ctx, r.containerName, path, nil)
	if err != nil {
		// Skip 404 not found error.
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound && skipNotFound {
			return
		}

		errorsCh <- fmt.Errorf("failed to open file %s: %w", path, err)

		return
	}

	if resp.Body != nil {
		readersCh <- models.File{Reader: resp.Body, Name: filepath.Base(path)}
	}
}

// StreamFile opens a single file from GCP cloud storage and sends io.Readers to the `readersCh`
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

// shouldSkip performs check, is we should skip files.
func (r *Reader) shouldSkip(path, fileName string) bool {
	return (isDirectory(path, fileName) && !r.withNestedDir) ||
		isSkippedByStartAfter(r.startAfter, fileName)
}

// GetType return `gcpStorageType` type of storage. Used in logging.
func (r *Reader) GetType() string {
	return azureBlobType
}

// checkRestoreDirectory checks that the restore directory contains any file.
func (r *Reader) checkRestoreDirectory(ctx context.Context, path string) error {
	pager := r.client.NewListBlobsFlatPager(r.containerName, &azblob.ListBlobsFlatOptions{
		Prefix: &path,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to get next page: %w", err)
		}

		for _, blob := range page.Segment.BlobItems {
			// Skip files in folders.
			if r.shouldSkip(path, *blob.Name) {
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

	return fmt.Errorf("%s is empty", path)
}

// ListObjects list all object in the path.
func (r *Reader) ListObjects(ctx context.Context, path string) ([]string, error) {
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	result := make([]string, 0)

	pager := r.client.NewListBlobsFlatPager(r.containerName, &azblob.ListBlobsFlatOptions{
		Prefix: &path,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get next page: %w", err)
		}

		for _, blob := range page.Segment.BlobItems {
			// Skip files in folders.
			if r.shouldSkip(path, *blob.Name) {
				continue
			}

			if blob.Name != nil {
				if r.validator != nil {
					if err = r.validator.Run(*blob.Name); err != nil {
						continue
					}
				}

				result = append(result, *blob.Name)
			}
		}
	}

	return result, nil
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

func isDirectory(prefix, fileName string) bool {
	// If file name ends with / it is 100% dir.
	if strings.HasSuffix(fileName, "/") {
		return true
	}

	// If we look inside some folder.
	if strings.HasPrefix(fileName, prefix) {
		// For root folder we should add.
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}

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

// cleanPath is protection from incorrect input.
func cleanPath(path string) string {
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
