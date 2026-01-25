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

package s3

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const s3type = "s3"

// Client is an interface for *s3.Client. Used for testing purposes.
type Client interface {
	// CreateMultipartUpload initiates a multipart upload and returns an upload ID.
	CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options),
	) (*s3.CreateMultipartUploadOutput, error)
	// UploadPart uploads a part in a multipart upload.
	UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options),
	) (*s3.UploadPartOutput, error)
	// CompleteMultipartUpload completes a multipart upload by assembling previously uploaded parts.
	CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options),
	) (*s3.CompleteMultipartUploadOutput, error)
	// AbortMultipartUpload aborts a multipart upload.
	AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options),
	) (*s3.AbortMultipartUploadOutput, error)
	// ListObjectsV2 returns some or all objects in a bucket with pagination.
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options),
	) (*s3.ListObjectsV2Output, error)
	// DeleteObject removes an object from a bucket.
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options),
	) (*s3.DeleteObjectOutput, error)
	// HeadBucket checks if a bucket exists and you have permission to access it.
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options),
	) (*s3.HeadBucketOutput, error)
	// RestoreObject restores an archived copy of an object.
	RestoreObject(ctx context.Context, params *s3.RestoreObjectInput, optFns ...func(*s3.Options,
	)) (*s3.RestoreObjectOutput, error)
	// HeadObject retrieves metadata from an object without returning the object itself.
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options),
	) (*s3.HeadObjectOutput, error)
	// GetObject retrieves an object from a bucket.
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options),
	) (*s3.GetObjectOutput, error)
}
