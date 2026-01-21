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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

// azblobGetter is an interface for azblob client. Used for mocking tests.
type azblobGetter interface {
	// GetBlobProperties encapsulate chain: ServiceClient().NewContainerClient().NewBlobClient().GetProperties()
	GetBlobProperties(ctx context.Context, path string,
	) (blob.GetPropertiesResponse, error)
	// DownloadStream downloads a file with options.
	DownloadStream(ctx context.Context, container, path string, options *blob.DownloadStreamOptions,
	) (azblob.DownloadStreamResponse, error)
}

// azureBlobClient is a wrapper around Client, primarily for testing purposes.
type azureBlobClient struct {
	client          Client
	containerClient *container.Client
}

// newAzureBlobClient creates a new wrapper for Client for testing.
func newAzureBlobClient(client Client, containerClient *container.Client) azblobGetter {
	return &azureBlobClient{client: client, containerClient: containerClient}
}

// GetBlobProperties implements chain: ServiceClient().NewContainerClient().NewBlobClient().GetProperties()
func (a *azureBlobClient) GetBlobProperties(ctx context.Context, path string,
) (blob.GetPropertiesResponse, error) {
	return a.containerClient.
		NewBlobClient(path).
		GetProperties(ctx, nil)
}

// DownloadStream implements DownloadStream().
func (a *azureBlobClient) DownloadStream(
	ctx context.Context, containerName, path string, options *blob.DownloadStreamOptions,
) (azblob.DownloadStreamResponse, error) {
	return a.client.DownloadStream(ctx, containerName, path, options)
}

// rangeReader file encapsulates getting a file by range and file size logic. To use with retry reader.
type rangeReader struct {
	client    azblobGetter
	container string
	path      string
	etag      *azcore.ETag

	size int64
}

// newRangeReader creates a new file reader.
func newRangeReader(ctx context.Context, client azblobGetter, containerName, path string) (*rangeReader, error) {
	objProps, err := client.GetBlobProperties(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to get properties %s: %w", path, err)
	}

	size := int64(0)
	if objProps.ContentLength != nil {
		size = *objProps.ContentLength
	}

	return &rangeReader{
		client:    client,
		container: containerName,
		path:      path,
		size:      size,
		etag:      objProps.ETag,
	}, nil
}

// OpenRange opens a file by range.
func (r *rangeReader) OpenRange(ctx context.Context, offset, count int64) (io.ReadCloser, error) {
	resp, err := r.client.DownloadStream(ctx, r.container, r.path, r.getStreamOptions(offset, count))
	if err != nil {
		return nil, fmt.Errorf("failed to download stream %s: %w", r.path, err)
	}

	return resp.NewRetryReader(ctx, &azblob.RetryReaderOptions{}), nil
}

// GetSize returns file size.
func (r *rangeReader) GetSize() int64 {
	return r.size
}

// GetInfo returns file info for logging.
func (r *rangeReader) GetInfo() string {
	return fmt.Sprintf("%s:%s", r.container, r.path)
}

// getStreamOptions returns stream options for download stream.
func (r *rangeReader) getStreamOptions(offset, count int64) *blob.DownloadStreamOptions {
	// HTTPRange defines a range of bytes within an HTTP resource, starting at offset and
	// ending at offset+count. A zero-value HTTPRange indicates the entire resource. An HTTPRange
	// which has an offset and zero value count indicates from the offset to the resource's end.
	if offset == 0 && count == 0 {
		return nil
	}

	return &blob.DownloadStreamOptions{
		Range: blob.HTTPRange{
			Offset: offset,
			Count:  count,
		},
		AccessConditions: &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfMatch: r.etag,
			},
		},
	}
}
