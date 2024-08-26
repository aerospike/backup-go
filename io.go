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

package backup

import (
	"context"

	gcpStorage "cloud.google.com/go/storage"
	"github.com/aerospike/backup-go/io/aws/s3"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/gcp/storage"
	"github.com/aerospike/backup-go/io/local"
)

// NewWriterLocalDir initializes a writer to the local directory.
func NewWriterLocalDir(path string, removeFiles bool) (Writer, error) {
	return local.NewDirectoryWriter(removeFiles, local.WithDir(path))
}

// NewStreamingReaderLocalDir initializes a reader from the local directory.
// At the moment, we support only `EncoderTypeASB` Encoder type.
func NewStreamingReaderLocalDir(path string, eType EncoderType) (StreamingReader, error) {
	switch eType {
	// As at the moment only one `ASB` validator supported, we use such construction.
	case EncoderTypeASB:
		return local.NewDirectoryStreamingReader(asb.NewValidator(), local.WithDir(path))
	default:
		return local.NewDirectoryStreamingReader(asb.NewValidator(), local.WithDir(path))
	}
}

// NewWriterLocalFile initializes a writer to the local file.
func NewWriterLocalFile(path string, removeFiles bool) (Writer, error) {
	return local.NewDirectoryWriter(removeFiles, local.WithFile(path))
}

// NewStreamingReaderLocalFile initializes a reader from the local file.
// At the moment, we support only `EncoderTypeASB` Encoder type.
func NewStreamingReaderLocalFile(path string, eType EncoderType) (StreamingReader, error) {
	switch eType {
	// As at the moment only one `ASB` validator supported, we use such construction.
	case EncoderTypeASB:
		return local.NewDirectoryStreamingReader(asb.NewValidator(), local.WithFile(path))
	default:
		return local.NewDirectoryStreamingReader(asb.NewValidator(), local.WithFile(path))
	}
}

// NewWriterS3 initializes a writer to the S3 directory.
func NewWriterS3(ctx context.Context, cfg *s3.Config, removeFiles bool) (Writer, error) {
	return s3.NewWriter(ctx, cfg, removeFiles)
}

// NewStreamingReaderS3 initializes a reader from the S3 directory.
// At the moment, we support only `EncoderTypeASB` Encoder type.
func NewStreamingReaderS3(ctx context.Context, cfg *s3.Config,
	eType EncoderType) (StreamingReader, error) {
	switch eType {
	// As at the moment only one `ASB` validator supported, we use such construction.
	case EncoderTypeASB:
		return s3.NewStreamingReader(ctx, cfg, asb.NewValidator())
	default:
		return s3.NewStreamingReader(ctx, cfg, asb.NewValidator())
	}
}

// NewWriterGCP initializes a writer to the GCP storage.
func NewWriterGCP(
	ctx context.Context,
	client *gcpStorage.Client,
	bucketName string,
	folderName string,
	removeFiles bool,
) (Writer, error) {
	return storage.NewWriter(ctx, client, bucketName, folderName, removeFiles)
}

// NewStreamingReaderGCP initializes a reader from the GCP storage.
// At the moment, we support only `EncoderTypeASB` Encoder type.
func NewStreamingReaderGCP(
	ctx context.Context,
	client *gcpStorage.Client,
	bucketName string,
	folderName string,
	eType EncoderType,
) (StreamingReader, error) {
	switch eType {
	// As at the moment only one `ASB` validator supported, we use such construction.
	case EncoderTypeASB:
		return storage.NewStreamingReader(ctx, client, bucketName, folderName, asb.NewValidator())
	default:
		return storage.NewStreamingReader(ctx, client, bucketName, folderName, asb.NewValidator())
	}
}
