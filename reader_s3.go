package backup

import (
	"bufio"
	"context"
	"io"
	"os"

	"github.com/aerospike/backup-go/encoding"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3ReaderFactory struct {
	client   *s3.Client
	s3Config *S3Config
}

var _ ReaderFactory = (*S3ReaderFactory)(nil)

func NewS3ReaderFactory(config *S3Config, _ *encoding.ASBDecoderFactory) (*S3ReaderFactory, error) {
	client, err := newS3Client(config)
	if err != nil {
		return nil, err
	}

	return &S3ReaderFactory{
		client:   client,
		s3Config: config,
	}, nil
}

func (f *S3ReaderFactory) Readers() ([]io.ReadCloser, error) {
	fileCh, errCh := f.streamFiles()
	readers := make([]io.ReadCloser, 0)

	for {
		select {
		case file, ok := <-fileCh:
			if !ok {
				fileCh = nil
			} else {
				reader, err := f.newS3Reader(file)
				if err != nil {
					return nil, err
				}

				readers = append(readers, reader)
			}

		case err, ok := <-errCh:
			if ok {
				return nil, err
			}

			errCh = nil
		}

		if fileCh == nil && errCh == nil {
			break
		}
	}

	return readers, nil
}

func (f *S3ReaderFactory) streamFiles() (files <-chan string, errors <-chan error) {
	fileCh := make(chan string)
	errCh := make(chan error)

	go func() {
		defer close(fileCh)
		defer close(errCh)

		var continuationToken *string

		for {
			listResponse, err := f.client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
				Bucket:            &f.s3Config.Bucket,
				Prefix:            &f.s3Config.Prefix,
				ContinuationToken: continuationToken,
			})
			if err != nil {
				errCh <- err
				return
			}

			for _, p := range listResponse.Contents {
				// TODO: use decoder to filter files.
				fileCh <- *p.Key
			}

			continuationToken = listResponse.NextContinuationToken
			if continuationToken == nil {
				break
			}
		}
	}()

	return fileCh, errCh
}

type S3Reader struct {
	body       io.Reader
	bucketName string
	key        string
	closed     bool
}

var _ io.ReadCloser = (*S3Reader)(nil)

func (f *S3ReaderFactory) newS3Reader(key string) (*S3Reader, error) {
	getObjectOutput, err := f.client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &f.s3Config.Bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}

	chunkSize := f.s3Config.ChunkSize
	if chunkSize == 0 {
		chunkSize = s3DefaultChunkSize
	}

	bufferedReader := bufio.NewReaderSize(getObjectOutput.Body, chunkSize)

	return &S3Reader{
		body:       bufferedReader,
		bucketName: f.s3Config.Bucket,
		key:        key,
	}, nil
}

func (r *S3Reader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, os.ErrClosed
	}

	return r.body.Read(p)
}

func (r *S3Reader) Close() error {
	if r.closed {
		return os.ErrClosed
	}

	r.closed = true

	return nil
}

func (f *S3ReaderFactory) GetType() string {
	return s3type
}
