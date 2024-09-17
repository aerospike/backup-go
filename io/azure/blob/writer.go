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
	"strings"
	"sync/atomic"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
)

const (
	uploadStreamFileType           = "application/octet-stream"
	uploadStreamBlockSize          = 5 * 1024 * 1024 // 5MB, minimum size of a part
	uploadStreamConcurrencyDefault = 5
)

// Writer represents a GCP storage writer.
type Writer struct {
	// Optional parameters.
	options

	client *azblob.Client
	// containerName contains name of the container to read from.
	containerName string
	// prefix contains folder name if we have folders inside the bucket.
	prefix string
	// Sync for running backup to one file.
	called atomic.Bool
}

// WithRemoveFiles adds remove files flag, so all files will be removed from backup folder before backup.
// Is used only for Writer.
func WithRemoveFiles() Opt {
	return func(r *options) {
		r.isRemovingFiles = true
	}
}

// WithUploadConcurrency define max number of concurrent uploads to be performed to upload the file.
// Is used only for Writer.
func WithUploadConcurrency(v int) Opt {
	return func(r *options) {
		r.uploadConcurrency = v
	}
}

func NewWriter(
	ctx context.Context,
	client *azblob.Client,
	containerName string,
	opts ...Opt,
) (*Writer, error) {
	w := &Writer{
		client: client,
	}
	// Set default value.
	w.uploadConcurrency = uploadStreamConcurrencyDefault

	for _, opt := range opts {
		opt(&w.options)
	}

	if w.path == "" {
		return nil, fmt.Errorf("path is required, use WithDir(path string) or WithFile(path string) to set")
	}

	var prefix string
	if w.isDir {
		prefix = w.path
		// Protection from incorrect input.
		if !strings.HasSuffix(w.path, "/") && w.path != "/" && w.path != "" {
			prefix = fmt.Sprintf("%s/", w.path)
		}
	}

	// Check if container exists.
	if _, err := client.ServiceClient().NewContainerClient(containerName).GetProperties(ctx, nil); err != nil {
		return nil, fmt.Errorf("unable to get container properties: %w", err)
	}

	// Check if backup dir is empty.
	isEmpty, err := isEmptyDirectory(ctx, client, containerName, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to check if directory is empty: %w", err)
	}

	if !isEmpty && !w.isRemovingFiles {
		return nil, fmt.Errorf("backup folder must be empty or set isRemovingFiles = true")
	}

	w.containerName = containerName
	w.prefix = prefix

	// As we accept only empty dir or dir with files for removing. We can remove them even in an empty bucket.
	if err = w.RemoveFiles(ctx); err != nil {
		return nil, fmt.Errorf("failed to remove files from folder: %w", err)
	}

	return w, nil
}

// NewWriter returns a new Azure blob writer to the specified path.
func (w *Writer) NewWriter(ctx context.Context, filename string) (io.WriteCloser, error) {
	// protection for single file backup.
	if !w.isDir {
		if !w.called.CompareAndSwap(false, true) {
			return nil, fmt.Errorf("parallel running for single file is not allowed")
		}
		// If we use backup to single file, we overwrite the file name.
		filename = w.path
	}

	filename = fmt.Sprintf("%s%s", w.prefix, filename)
	blockBlobClient := w.client.ServiceClient().NewContainerClient(w.containerName).NewBlockBlobClient(filename)

	return newBlobWriter(ctx, blockBlobClient, w.uploadConcurrency), nil
}

var _ io.WriteCloser = (*blobWriter)(nil)

// blobWriter wrapper for io.WriteCloser
type blobWriter struct {
	ctx               context.Context
	blobClient        *blockblob.Client
	pipeReader        *io.PipeReader
	pipeWriter        *io.PipeWriter
	done              chan error
	uploadConcurrency int
}

func newBlobWriter(ctx context.Context, blobClient *blockblob.Client, uploadConcurrency int) io.WriteCloser {
	pipeReader, pipeWriter := io.Pipe()

	w := &blobWriter{
		blobClient:        blobClient,
		ctx:               ctx,
		pipeReader:        pipeReader,
		pipeWriter:        pipeWriter,
		done:              make(chan error, 1),
		uploadConcurrency: uploadConcurrency,
	}

	go w.uploadStream()

	return w
}

func (w *blobWriter) uploadStream() {
	contentType := uploadStreamFileType
	_, err := w.blobClient.UploadStream(w.ctx, w.pipeReader, &azblob.UploadStreamOptions{
		BlockSize:   uploadStreamBlockSize,
		Concurrency: w.uploadConcurrency,
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: &contentType,
		}})
	w.done <- err
	close(w.done)
}

func (w *blobWriter) Write(p []byte) (int, error) {
	return w.pipeWriter.Write(p)
}

func (w *blobWriter) Close() error {
	err := w.pipeWriter.Close()
	if err != nil {
		return err
	}

	return <-w.done
}

// GetType return `gcpStorageType` type of storage. Used in logging.
func (w *Writer) GetType() string {
	return azureBlobType
}

func isEmptyDirectory(ctx context.Context, client *azblob.Client, containerName, prefix string) (bool, error) {
	maxResults := int32(1)
	pager := client.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
		Prefix:     &prefix,
		MaxResults: &maxResults,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return false, fmt.Errorf("failed to get next page: %w", err)
		}

		if len(page.Segment.BlobItems) == 0 {
			return true, nil
		}
		// For nested folders azure return folder itself.
		if *page.Segment.BlobItems[0].Name == prefix {
			return true, nil
		}
	}

	return false, nil
}

// RemoveFiles removes a backup file or files from directory.
func (w *Writer) RemoveFiles(ctx context.Context) error {
	// Remove file.
	if !w.isDir {
		_, err := w.client.DeleteBlob(ctx, w.containerName, w.path, nil)
		if err != nil {
			return fmt.Errorf("failed to delete blob %s: %w", w.path, err)
		}

		return nil
	}
	// Remove files from dir.
	pager := w.client.NewListBlobsFlatPager(w.containerName, &azblob.ListBlobsFlatOptions{
		Prefix: &w.prefix,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to get next page: %w", err)
		}

		for _, blob := range page.Segment.BlobItems {
			// Skip files in folders.
			if isDirectory(w.prefix, *blob.Name) {
				continue
			}

			_, err = w.client.DeleteBlob(ctx, w.containerName, *blob.Name, nil)
			if err != nil {
				return fmt.Errorf("failed to delete blob %s: %w", *blob.Name, err)
			}
		}
	}

	return nil
}
