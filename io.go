package backup

import (
	"context"

	"github.com/aerospike/backup-go/io/aws/s3"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/local"
	"github.com/aerospike/backup-go/models"
)

// NewWriterLocal initialize a writer for local directory.
func NewWriterLocal(dir string, removeFiles bool) (Writer, error) {
	return local.NewDirectoryWriterFactory(dir, removeFiles)
}

// NewStreamingReaderLocal initialize reader from the local directory.
// At the moment we have one Encoder type, so use `EncoderTypeASB`.
func NewStreamingReaderLocal(dir string, eType EncoderType) (StreamingReader, error) {
	switch eType {
	// As at the moment only one `ASB` validator supported, we use such construction.
	case EncoderTypeASB:
		return local.NewDirectoryStreamingReader(dir, asb.NewValidator())
	default:
		return local.NewDirectoryStreamingReader(dir, asb.NewValidator())
	}
}

// NewWriterS3 initialize a writer for s3 directory.
func NewWriterS3(ctx context.Context, cfg *models.S3Config, removeFiles bool) (Writer, error) {
	return s3.NewWriter(ctx, cfg, removeFiles)
}

// NewStreamingReaderS3 initialize reader from the s3 directory.
// At the moment we have one Encoder type, so use `EncoderTypeASB`.
func NewStreamingReaderS3(ctx context.Context, cfg *models.S3Config, eType EncoderType) (StreamingReader, error) {
	switch eType {
	// As at the moment only one `ASB` validator supported, we use such construction.
	case EncoderTypeASB:
		return s3.NewStreamingReader(ctx, cfg, asb.NewValidator())
	default:
		return s3.NewStreamingReader(ctx, cfg, asb.NewValidator())
	}
}
