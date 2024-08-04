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
	Bucket    string
	Region    string
	Endpoint  string
	Profile   string
	Prefix    string
	ChunkSize int
}

// NewConfig returns new AWS S3 configuration.
func NewConfig(
	bucket string,
	region string,
	endpoint string,
	profile string,
	prefix string,
	chunkSize int,
) *Config {
	return &Config{
		Bucket:    bucket,
		Region:    region,
		Endpoint:  endpoint,
		Profile:   profile,
		Prefix:    prefix,
		ChunkSize: chunkSize,
	}
}
