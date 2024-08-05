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

// Config represents the AWS S3 configuration.
type Config struct {
	// The S3 bucket to store backup.
	Bucket string
	// Folder name for the backup.
	Prefix string

	// The following property names are identical with the C library

	// The S3 region that the bucket(s) exist in.
	Region string
	// The S3 profile to use for credentials.
	Profile string
	// An alternate url endpoint to send S3 API calls to.
	Endpoint string
	// The minimum size in bytes of individual S3 UploadParts.
	MinPartSize int
	// The maximum number of simultaneous download requests from S3.
	MaxAsyncDownloads int
	// The maximum number of simultaneous upload requests from S3.
	MaxAsyncUploads int
}

// NewConfig returns new AWS S3 configuration.
func NewConfig(
	bucket string,
	region string,
	endpoint string,
	profile string,
	prefix string,
	minPartSize int,
	maxAsyncDownloads int,
	maxAsyncUploads int,
) *Config {
	return &Config{
		Bucket:            bucket,
		Region:            region,
		Endpoint:          endpoint,
		Profile:           profile,
		Prefix:            prefix,
		MinPartSize:       minPartSize,
		MaxAsyncDownloads: maxAsyncDownloads,
		MaxAsyncUploads:   maxAsyncUploads,
	}
}
