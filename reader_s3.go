package backup

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3ReaderFactory struct {
	client   *s3.Client
	s3Config *S3Config
	decoder  DecoderFactory
}

var _ ReaderFactory = (*S3ReaderFactory)(nil)

func NewS3ReaderFactory(config *S3Config, decoder DecoderFactory) (*S3ReaderFactory, error) {
	client, err := newS3Client(config)
	if err != nil {
		return nil, err
	}

	return &S3ReaderFactory{
		client:   client,
		s3Config: config,
		decoder:  decoder,
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

	if len(readers) == 0 {
		return nil, fmt.Errorf("%w: %s doesn't contain backup files", ErrRestoreDirectoryInvalid, f.s3Config.Prefix)
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
			prefix := strings.Trim(f.s3Config.Prefix, "/") + "/"
			listResponse, err := f.client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
				Bucket:            &f.s3Config.Bucket,
				Prefix:            &prefix,
				ContinuationToken: continuationToken,
			})

			if err != nil {
				errCh <- err
				return
			}

			for _, p := range listResponse.Contents {
				if err := verifyBackupFileExtension(*p.Key, f.decoder); err != nil {
					continue
				}
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
	reader io.Reader
	closer io.Closer
	closed bool
}

var _ io.ReadCloser = (*S3Reader)(nil)

func (f *S3ReaderFactory) newS3Reader(key string) (io.ReadCloser, error) {
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

	return &S3Reader{
		reader: bufio.NewReaderSize(getObjectOutput.Body, chunkSize),
		closer: getObjectOutput.Body,
	}, nil
}

func (r *S3Reader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, os.ErrClosed
	}

	return r.reader.Read(p)
}

func (r *S3Reader) Close() error {
	if r.closed {
		return os.ErrClosed
	}

	r.closed = true

	return r.closer.Close()
}

func (f *S3ReaderFactory) GetType() string {
	return s3type
}
