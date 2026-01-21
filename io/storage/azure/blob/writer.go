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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/aerospike/backup-go/io/storage/common"
	"github.com/aerospike/backup-go/io/storage/options"
)

const (
	uploadStreamFileType           = "application/octet-stream"
	uploadStreamBlockSize          = 5 * 1024 * 1024
	uploadStreamConcurrencyDefault = 5
)

// Writer represents a Azure storage writer.
type Writer struct {
	// Optional parameters.
	options.Options

	client          Client
	containerClient *container.Client
	// containerName contains name of the container to read from.
	containerName string
	// prefix contains folder name if we have folders inside the bucket.
	prefix string

	tier *blob.AccessTier
}

// NewWriter creates a new writer for Azure blob storage directory/file writing.
func NewWriter(
	ctx context.Context,
	client Client,
	containerName string,
	opts ...options.Opt,
) (*Writer, error) {
	w := &Writer{
		client: client,
	}

	for _, opt := range opts {
		opt(&w.Options)
	}

	if w.ChunkSize < 0 {
		return nil, fmt.Errorf("chunk size must be positive")
	}

	// Set default value.
	w.UploadConcurrency = uploadStreamConcurrencyDefault
	if w.ChunkSize == 0 {
		w.ChunkSize = uploadStreamBlockSize
	}

	if len(w.PathList) != 1 {
		return nil, fmt.Errorf("one path is required, use WithDir(path string) or WithFile(path string) to set")
	}

	if w.IsDir {
		w.prefix = common.CleanPath(w.PathList[0], false)
	}

	// Check if a container exists.
	w.containerClient = client.ServiceClient().NewContainerClient(containerName)
	if _, err := w.containerClient.GetProperties(ctx, nil); err != nil {
		return nil, fmt.Errorf("unable to get container properties: %w", err)
	}

	if w.IsDir && !w.SkipDirCheck {
		// Check if backup dir is empty.
		isEmpty, err := isEmptyDirectory(ctx, client, containerName, w.prefix)
		if err != nil {
			return nil, fmt.Errorf("failed to check if directory is empty: %w", err)
		}

		if !isEmpty && !w.IsRemovingFiles {
			return nil, fmt.Errorf("backup folder must be empty or set RemoveFiles = true")
		}
	}

	w.containerName = containerName

	if w.IsRemovingFiles {
		// As we accept only empty dir or dir with files for removing. We can remove them even in an empty bucket.
		if err := w.RemoveFiles(ctx); err != nil {
			return nil, fmt.Errorf("failed to remove files from folder: %w", err)
		}
	}

	if w.StorageClass != "" {
		// validation.
		tier, err := parseAccessTier(w.StorageClass)
		if err != nil {
			return nil, fmt.Errorf("failed to parse access tier: %w", err)
		}

		w.tier = &tier
	}

	return w, nil
}

// NewWriter returns a new Azure blob writer to the specified path.
// isRecords indicates if the file contains record data.
func (w *Writer) NewWriter(ctx context.Context, filename string) (io.WriteCloser, error) {
	// Get the full path for the file.
	fullPath, err := common.GetFullPath(w.prefix, filename, w.PathList, w.IsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get full path: %w", err)
	}

	blockBlobClient := w.containerClient.NewBlockBlobClient(fullPath)

	return newBlobWriter(ctx, blockBlobClient, w.UploadConcurrency, w.tier, int64(w.ChunkSize), w.WithChecksum), nil
}

var _ io.WriteCloser = (*blobWriter)(nil)

// blobWriter wrapper for io.WriteCloser
type blobWriter struct {
	ctx               context.Context
	blobClient        *blockblob.Client
	tier              *blob.AccessTier
	pipeReader        *io.PipeReader
	pipeWriter        *io.PipeWriter
	bw                *bufio.Writer
	closeOnce         sync.Once
	done              chan error
	uploadConcurrency int
	chunkSize         int64
	withChecksum      bool
}

// newBlobWriter creates a new blob writer.
func newBlobWriter(
	ctx context.Context,
	blobClient *blockblob.Client,
	uploadConcurrency int,
	tier *blob.AccessTier,
	chunkSize int64,
	withChecksum bool,
) io.WriteCloser {
	pipeReader, pipeWriter := io.Pipe()

	w := &blobWriter{
		ctx:               ctx,
		blobClient:        blobClient,
		tier:              tier,
		pipeReader:        pipeReader,
		pipeWriter:        pipeWriter,
		bw:                bufio.NewWriterSize(pipeWriter, common.SafeInt64ToInt(chunkSize)),
		done:              make(chan error, 1),
		uploadConcurrency: uploadConcurrency,
		chunkSize:         chunkSize,
		withChecksum:      withChecksum,
	}

	go w.uploadStream()

	return w
}

func (w *blobWriter) uploadStream() {
	defer close(w.done)

	contentType := uploadStreamFileType

	usOpt := &azblob.UploadStreamOptions{
		BlockSize:   w.chunkSize,
		Concurrency: w.uploadConcurrency,
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: &contentType,
		},
		AccessTier: w.tier,
	}

	if w.withChecksum {
		usOpt.TransactionalValidation = blob.TransferValidationTypeComputeCRC64()
	}

	_, err := w.blobClient.UploadStream(
		w.ctx,
		w.pipeReader,
		usOpt,
	)

	cErr := w.pipeReader.CloseWithError(err)

	if err != nil || (cErr != nil && !errors.Is(cErr, io.ErrClosedPipe)) {
		err = errors.Join(err, cErr)
	}

	w.done <- err
}

// Write writes the data to the writer.
func (w *blobWriter) Write(p []byte) (int, error) {
	return w.bw.Write(p)
}

// Close closes the writer.
func (w *blobWriter) Close() error {
	var err error

	w.closeOnce.Do(func() {
		flushErr := w.bw.Flush()
		closeErr := w.pipeWriter.Close()
		uploadErr := <-w.done

		err = errors.Join(flushErr, closeErr, uploadErr)
	})

	return err
}

// GetType return `azureBlobType` type of storage. Used in logging.
func (w *Writer) GetType() string {
	return azureBlobType
}

func isEmptyDirectory(ctx context.Context, client Client, containerName, prefix string) (bool, error) {
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
	return w.Remove(ctx, w.PathList[0])
}

// Remove deletes the file or directory contents specified by path.
func (w *Writer) Remove(ctx context.Context, targetPath string) error {
	// Remove file.
	if !w.IsDir {
		_, err := w.client.DeleteBlob(ctx, w.containerName, targetPath, nil)
		if err != nil {
			return fmt.Errorf("failed to delete blob %s: %w", targetPath, err)
		}

		return nil
	}

	prefix := common.CleanPath(targetPath, false)
	// Remove files from dir.
	pager := w.client.NewListBlobsFlatPager(w.containerName, &azblob.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to get next page: %w", err)
		}

		for _, blobItem := range page.Segment.BlobItems {
			// Skip files in folders.
			if common.IsDirectory(prefix, *blobItem.Name) && !w.WithNestedDir {
				continue
			}

			// If validator is set, remove only valid files.
			if w.Validator != nil {
				if err = w.Validator.Run(*blobItem.Name); err != nil {
					continue
				}
			}

			_, err = w.client.DeleteBlob(ctx, w.containerName, *blobItem.Name, nil)
			if err != nil {
				return fmt.Errorf("failed to delete blobItem %s: %w", *blobItem.Name, err)
			}
		}
	}

	return nil
}

// GetOptions returns initialized options for the writer.
func (w *Writer) GetOptions() options.Options {
	return w.Options
}
