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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
)

const azureBlobType = "azure-blob"

// Client is an interface for *azblob.Client. Used for testing purposes.
type Client interface {
	// ServiceClient returns a pointer to the container's service.Client.
	ServiceClient() *service.Client
	// NewListBlobsFlatPager returns a pager for listing blobs in a container.
	NewListBlobsFlatPager(
		containerName string,
		o *azblob.ListBlobsFlatOptions,
	) *runtime.Pager[azblob.ListBlobsFlatResponse]
	// DeleteBlob deletes a blob from the container.
	DeleteBlob(
		ctx context.Context,
		containerName string,
		blobName string,
		o *azblob.DeleteBlobOptions,
	) (azblob.DeleteBlobResponse, error)
	// DownloadStream reads a blob from the container into a stream.
	DownloadStream(
		ctx context.Context,
		containerName string,
		blobName string,
		o *azblob.DownloadStreamOptions,
	) (azblob.DownloadStreamResponse, error)
}
