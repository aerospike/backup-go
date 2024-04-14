package backup

import (
	"bytes"
	"io"
	"os"
	"path"
	"sync/atomic"

	"github.com/aerospike/backup-go/encoding"
	"github.com/aerospike/backup-go/encoding/asb"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3Writer struct {
	key      string
	uploader *s3manager.Uploader
	config   *S3Config
	buffer   bytes.Buffer
	closed   bool
}

type S3WriteFactory struct {
	config  *S3Config
	fileID  *atomic.Int32
	encoder encoding.Encoder
}

func NewS3WriterFactory(config *S3Config, encoder encoding.Encoder) *S3WriteFactory {
	return &S3WriteFactory{
		config:  config,
		fileID:  &atomic.Int32{},
		encoder: encoder,
	}
}

func (s *S3WriteFactory) NewWriter(namespace string) (io.WriteCloser, error) {
	var name string
	if _, ok := s.encoder.(*asb.Encoder); ok {
		name = getBackupFileNameASB(namespace, int(s.fileID.Add(1)))
		fullPath := path.Join(s.config.Prefix, name)

		writer, err := NewS3Writer(s.config, fullPath)
		if err != nil {
			return nil, err
		}

		err = writeASBHeader(writer, namespace, s.fileID.Load() == 1)
		if err != nil {
			return nil, err
		}

		return writer, nil
	}

	name = getBackupFileNameGeneric(namespace, int(s.fileID.Add(1)))
	fullPath := path.Join(s.config.Prefix, name)

	return NewS3Writer(s.config, fullPath)
}

func NewS3Writer(config *S3Config, key string) (*S3Writer, error) {
	sess, err := NewSession(config)
	if err != nil {
		return nil, err
	}

	uploader := s3manager.NewUploader(sess)

	return &S3Writer{
		config:   config,
		uploader: uploader, key: key,
	}, nil
}

func (w *S3Writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, os.ErrClosed
	}

	return w.buffer.Write(p)
}

func (w *S3Writer) Close() error {
	if w.closed {
		return os.ErrClosed
	}

	input := &s3manager.UploadInput{
		Bucket: aws.String(w.config.Bucket),
		Key:    aws.String(w.key),
		Body:   bytes.NewReader(w.buffer.Bytes()),
	}

	_, err := w.uploader.Upload(input)
	if err != nil {
		return err
	}

	w.closed = true

	return nil
}
