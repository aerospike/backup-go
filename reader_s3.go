package backup

import (
	"io"

	"github.com/aerospike/backup-go/encoding"
	"github.com/aerospike/backup-go/internal/logging"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3ReaderFactory struct {
	config *S3Config
}

var _ ReaderFactory = (*S3ReaderFactory)(nil)

func (f *S3ReaderFactory) Readers() ([]io.ReadCloser, error) {
	sess, err := NewSession(f.config)
	if err != nil {
		return nil, err
	}

	resp, err := s3.New(sess).ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(f.config.Bucket),
		Prefix: aws.String(f.config.Prefix),
	})
	if err != nil {
		return nil, err
	}

	readers := make([]io.ReadCloser, 0, len(resp.Contents))

	for _, item := range resp.Contents {
		reader, err := NewS3Reader(f.config, *item.Key)
		if err != nil {
			return nil, err
		}

		readers = append(readers, reader)
	}

	return readers, nil
}

func NewS3ReaderFactory(config *S3Config, _ *encoding.ASBDecoderFactory) *S3ReaderFactory {
	// TODO: use decoder to filter files.
	return &S3ReaderFactory{config: config}
}

type S3Reader struct {
	config          *S3Config
	downloader      *s3manager.Downloader
	buffer          *aws.WriteAtBuffer
	key             string
	bytesDownloaded bool
}

func NewS3Reader(config *S3Config, key string) (*S3Reader, error) {
	sess, err := NewSession(config)
	if err != nil {
		return nil, err
	}

	return &S3Reader{
		config:     config,
		downloader: s3manager.NewDownloader(sess),
		buffer:     aws.NewWriteAtBuffer([]byte{}),
		key:        key,
	}, nil
}

func (r *S3Reader) Read(p []byte) (int, error) {
	// TODO: read with pagination
	if !r.bytesDownloaded {
		_, err := r.downloader.Download(r.buffer, &s3.GetObjectInput{
			Bucket: aws.String(r.config.Bucket),
			Key:    aws.String(r.key),
		})
		if err != nil {
			return 0, err
		}

		r.bytesDownloaded = true
	}

	b := r.buffer.Bytes()
	if len(b) == 0 {
		return 0, io.EOF
	}

	n := copy(p, b)
	b = b[n:]
	r.buffer = aws.NewWriteAtBuffer(b)

	return n, nil
}

func (r *S3Reader) Close() error {
	return nil
}

func (f *S3ReaderFactory) GetType() logging.HandlerType {
	return logging.HandlerTypeRestoreS3
}
