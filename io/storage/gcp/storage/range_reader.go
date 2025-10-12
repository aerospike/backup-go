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
	"fmt"
	"io"

	"cloud.google.com/go/storage"
)

type gcpGetter interface {
	GetAttrs(ctx context.Context, path string) (attrs *storage.ObjectAttrs, err error)
	GetReader(ctx context.Context, path string, generation, offset, length int64) (reader io.ReadCloser, err error)
}

// gcpStorageClient wrapper for *storage.Client for testing.
type gcpStorageClient struct {
	handler *storage.BucketHandle
}

// newGcpStorageClient creates a new wrapper for *storage.Client for testing.
func newGcpStorageClient(handler *storage.BucketHandle) gcpGetter {
	return &gcpStorageClient{handler: handler}
}

// GetAttrs returns object attributes.
func (g *gcpStorageClient) GetAttrs(ctx context.Context, path string) (attrs *storage.ObjectAttrs, err error) {
	return g.handler.Object(path).Attrs(ctx)
}

func (g *gcpStorageClient) GetReader(ctx context.Context, path string, generation, offset, length int64) (reader io.ReadCloser, err error) {
	return g.handler.Object(path).Generation(generation).NewRangeReader(ctx, offset, length)
}

// rangeReader encapsulate getting a file by range and file size logic. To use with retry reader.
type rangeReader struct {
	client     gcpGetter
	bucket     string
	path       string
	generation int64

	size int64
}

// newRangeReader creates a new file reader.
func newRangeReader(ctx context.Context, client gcpGetter, bucket, path string) (*rangeReader, error) {
	head, err := client.GetAttrs(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to get attr %s: %w", path, err)
	}

	return &rangeReader{
		client:     client,
		bucket:     bucket,
		path:       path,
		generation: head.Generation,
		size:       head.Size,
	}, nil
}

// OpenRange opens a file by range.
func (r *rangeReader) OpenRange(ctx context.Context, offset, count int64) (io.ReadCloser, error) {
	resp, err := r.client.GetReader(ctx, r.path, r.generation, offset, count)
	if err != nil {
		return nil, fmt.Errorf("failed to get reader %s: %w", r.path, err)
	}

	return resp, nil
}

// GetSize returns file size.
func (r *rangeReader) GetSize() int64 {
	return r.size
}

// GetInfo returns file info for logging.
func (r *rangeReader) GetInfo() string {
	return fmt.Sprintf("%s:%s", r.bucket, r.path)
}
