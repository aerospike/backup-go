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

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/encoding"
	"github.com/aerospike/backup-go/internal/writers"
	"github.com/aerospike/backup-go/io/local"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type s3WriteFactory struct {
	client   *s3.Client
	s3Config *StorageConfig
	fileID   *atomic.Uint32 // increments for each new file created
	encoder  encoding.EncoderFactory
}

var _ backup.WriteFactory = (*s3WriteFactory)(nil)

func NewS3WriterFactory(
	s3Config *StorageConfig,
	encoder encoding.EncoderFactory,
	removeFiles bool,
) (backup.WriteFactory, error) {
	if encoder == nil {
		return nil, errors.New("encoder is nil")
	}

	if s3Config.ChunkSize > maxS3File {
		return nil, fmt.Errorf("invalid chunk size %d, should not exceed %d", s3Config.ChunkSize, maxS3File)
	}

	client, err := newS3Client(s3Config)
	if err != nil {
		return nil, err
	}

	isEmpty, err := isEmptyDirectory(client, s3Config)
	if err != nil {
		return nil, err
	}

	if !isEmpty {
		if !removeFiles {
			return nil, fmt.Errorf("%w: %s is not empty", local.ErrBackupDirectoryInvalid, s3Config.Prefix)
		}

		err := deleteAllFilesUnderPrefix(client, s3Config)
		if err != nil {
			return nil, err
		}
	}

	return &s3WriteFactory{
		client:   client,
		s3Config: s3Config,
		fileID:   &atomic.Uint32{},
		encoder:  encoder,
	}, nil
}

type s3Writer struct {
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

func (f *s3WriteFactory) NewWriter(namespace string, writeHeader func(io.WriteCloser) error) (io.WriteCloser, error) {
	chunkSize := f.s3Config.ChunkSize
	if chunkSize < s3DefaultChunkSize {
		chunkSize = s3DefaultChunkSize
	}

	var open = func() (io.WriteCloser, error) {
		name := f.encoder.GenerateFilename(namespace, f.fileID.Add(1))
		fullPath := path.Join(f.s3Config.Prefix, name)

		upload, err := f.client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
			Bucket: &f.s3Config.Bucket,
			Key:    &fullPath,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create multipart upload: %w", err)
		}

		writer := &s3Writer{
			uploadID:   upload.UploadId,
			key:        fullPath,
			client:     f.client,
			bucket:     f.s3Config.Bucket,
			buffer:     new(bytes.Buffer),
			partNumber: 1,
			chunkSize:  chunkSize,
		}

		err = writeHeader(writer)
		if err != nil {
			return nil, err
		}

		return writer, nil
	}

	writer, err := open()
	if err != nil {
		return nil, fmt.Errorf("failed to create backup file writer: %w", err)
	}

	return writers.NewSized(maxS3File, writer, open), nil
}

func (f *s3WriteFactory) GetType() string {
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
	response, err := w.client.UploadPart(context.TODO(), &s3.UploadPartInput{
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

	_, err := w.client.CompleteMultipartUpload(context.TODO(),
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

func isEmptyDirectory(client *s3.Client, s3config *StorageConfig) (bool, error) {
	resp, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
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

func deleteAllFilesUnderPrefix(client *s3.Client, s3config *StorageConfig) error {
	fileCh, errCh := streamFilesFromS3(client, s3config)

	for {
		select {
		case file, ok := <-fileCh:
			if !ok {
				fileCh = nil // no more files
			} else {
				_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
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
