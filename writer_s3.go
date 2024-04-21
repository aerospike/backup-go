package backup

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync/atomic"

	"github.com/aerospike/backup-go/encoding"
	"github.com/aerospike/backup-go/internal/writers"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3WriteFactory struct {
	client   *s3.Client
	s3Config *S3Config
	fileID   *atomic.Int32 // increments for each new file created
	encoder  EncoderFactory
}

var _ WriteFactory = (*S3WriteFactory)(nil)

func NewS3WriterFactory(s3Config *S3Config, encoder EncoderFactory) (*S3WriteFactory, error) {
	if s3Config.ChunkSize > maxS3File {
		return nil, fmt.Errorf("invalid chunk size %d, should not exceed %d", s3Config.ChunkSize, maxS3File)
	}

	client, err := newS3Client(s3Config)
	if err != nil {
		return nil, err
	}

	return &S3WriteFactory{
		client:   client,
		s3Config: s3Config,
		fileID:   &atomic.Int32{},
		encoder:  encoder,
	}, nil
}

type S3Writer struct {
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

var _ io.WriteCloser = (*S3Writer)(nil)

func (f *S3WriteFactory) NewWriter(namespace string, writeHeader func(io.WriteCloser) error) (io.WriteCloser, error) {
	chunkSize := f.s3Config.ChunkSize
	if chunkSize < s3DefaultChunkSize {
		chunkSize = s3DefaultChunkSize
	}

	var open = func() (io.WriteCloser, error) {
		var name string
		if _, ok := f.encoder.(*encoding.ASBEncoderFactory); ok {
			name = getBackupFileNameASB(namespace, int(f.fileID.Add(1)))
		} else {
			name = getBackupFileNameGeneric(namespace, int(f.fileID.Add(1)))
		}

		fullPath := path.Join(f.s3Config.Prefix, name)

		upload, err := f.client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
			Bucket: &f.s3Config.Bucket,
			Key:    &fullPath,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create multipart upload: %w", err)
		}

		writer := &S3Writer{
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

func (f *S3WriteFactory) GetType() string {
	return s3type
}

func (w *S3Writer) Write(p []byte) (int, error) {
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

func (w *S3Writer) uploadPart() error {
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

func (w *S3Writer) Close() error {
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
		return fmt.Errorf("failed to complete multipart upload, %v", err)
	}

	w.closed = true

	return nil
}
