package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync/atomic"

	"github.com/aerospike/backup-go/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var errBackupDirectoryInvalid = errors.New("backup directory is invalid")

type Writer struct {
	client   *s3.Client
	s3Config *models.S3Config
	fileID   *atomic.Uint32 // increments for each new file created
}

func NewS3WriterFactory(
	ctx context.Context,
	s3Config *models.S3Config,
	removeFiles bool,
) (*Writer, error) {
	if s3Config.ChunkSize > maxS3File {
		return nil, fmt.Errorf("invalid chunk size %d, should not exceed %d", s3Config.ChunkSize, maxS3File)
	}

	client, err := newS3Client(ctx, s3Config)
	if err != nil {
		return nil, err
	}

	isEmpty, err := isEmptyDirectory(ctx, client, s3Config)
	if err != nil {
		return nil, err
	}

	if !isEmpty {
		if !removeFiles {
			return nil, fmt.Errorf("%w: %s is not empty", errBackupDirectoryInvalid, s3Config.Prefix)
		}

		err := deleteAllFilesUnderPrefix(ctx, client, s3Config)
		if err != nil {
			return nil, err
		}
	}

	return &Writer{
		client:   client,
		s3Config: s3Config,
		fileID:   &atomic.Uint32{},
	}, nil
}

type s3Writer struct {
	// ctx is stored internally so that it can be used in io.WriteCloser methods
	ctx            context.Context
	uploadID       *string
	client         *s3.Client
	buffer         *bytes.Buffer
	key            string
	bucket         string
	completedParts []types.CompletedPart
	chunkSize      int
	partNumber     int32
	closed         bool
}

var _ io.WriteCloser = (*s3Writer)(nil)

func (f *Writer) NewWriter(ctx context.Context, filename string) (io.WriteCloser, error) {
	chunkSize := f.s3Config.ChunkSize
	if chunkSize < s3DefaultChunkSize {
		chunkSize = s3DefaultChunkSize
	}

	fullPath := path.Join(f.s3Config.Prefix, filename)

	upload, err := f.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &f.s3Config.Bucket,
		Key:    &fullPath,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create multipart upload: %w", err)
	}

	return &s3Writer{
		ctx:        ctx,
		uploadID:   upload.UploadId,
		key:        fullPath,
		client:     f.client,
		bucket:     f.s3Config.Bucket,
		buffer:     new(bytes.Buffer),
		partNumber: 1,
		chunkSize:  chunkSize,
	}, nil
}

func (f *Writer) GetType() string {
	return s3type
}

func (w *s3Writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, os.ErrClosed
	}

	if w.buffer.Len() >= w.chunkSize {
		err := w.uploadPart()
		if err != nil {
			return 0, err
		}
	}

	return w.buffer.Write(p)
}

func (w *s3Writer) uploadPart() error {
	response, err := w.client.UploadPart(w.ctx, &s3.UploadPartInput{
		Body:       bytes.NewReader(w.buffer.Bytes()),
		Bucket:     &w.bucket,
		Key:        &w.key,
		PartNumber: &w.partNumber,
		UploadId:   w.uploadID,
	})

	if err != nil {
		return fmt.Errorf("failed to upload part, %w", err)
	}

	p := w.partNumber
	w.completedParts = append(w.completedParts, types.CompletedPart{
		PartNumber: &p,
		ETag:       response.ETag,
	})

	w.partNumber++
	w.buffer.Reset()

	return nil
}

func (w *s3Writer) Close() error {
	if w.closed {
		return os.ErrClosed
	}

	if w.buffer.Len() > 0 {
		err := w.uploadPart()
		if err != nil {
			return err
		}
	}

	_, err := w.client.CompleteMultipartUpload(w.ctx,
		&s3.CompleteMultipartUploadInput{
			Bucket:   &w.bucket,
			UploadId: w.uploadID,
			Key:      &w.key,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: w.completedParts,
			},
		})
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload, %w", err)
	}

	w.closed = true

	return nil
}

func isEmptyDirectory(ctx context.Context, client *s3.Client, s3config *models.S3Config) (bool, error) {
	resp, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  &s3config.Bucket,
		Prefix:  &s3config.Prefix,
		MaxKeys: aws.Int32(1),
	})

	if err != nil {
		return false, err
	}

	// Check if it's a single object
	if len(resp.Contents) == 1 && *resp.Contents[0].Key == s3config.Prefix {
		return false, nil
	}

	return len(resp.Contents) == 0, nil
}

func deleteAllFilesUnderPrefix(ctx context.Context, client *s3.Client, s3config *models.S3Config) error {
	fileCh, errCh := streamFilesFromS3(ctx, client, s3config)

	for {
		select {
		case file, ok := <-fileCh:
			if !ok {
				fileCh = nil // no more files
			} else {
				_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(s3config.Bucket),
					Key:    aws.String(file),
				})
				if err != nil {
					return err
				}
			}
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
			} else {
				return err
			}
		}

		if fileCh == nil && errCh == nil { // if no more files and no more errors
			break
		}
	}

	return nil
}
