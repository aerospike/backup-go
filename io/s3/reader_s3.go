package s3

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/encoding"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3ReaderFactory struct {
	client   *s3.Client
	s3Config *StorageConfig
	decoder  encoding.DecoderFactory
}

var _ backup.ReaderFactory = (*s3ReaderFactory)(nil)

func NewS3ReaderFactory(config *StorageConfig, decoder encoding.DecoderFactory) (backup.ReaderFactory, error) {
	if decoder == nil {
		return nil, errors.New("decoder is nil")
	}

	client, err := newS3Client(config)
	if err != nil {
		return nil, err
	}

	return &s3ReaderFactory{
		client:   client,
		s3Config: config,
		decoder:  decoder,
	}, nil
}

func (f *s3ReaderFactory) Readers() ([]io.ReadCloser, error) {
	fileCh, errCh := f.streamBackupFiles()
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
		return nil, fmt.Errorf("%w: %s doesn't contain backup files", backup.ErrRestoreDirectoryInvalid, f.s3Config.Prefix)
	}

	return readers, nil
}

func (f *s3ReaderFactory) streamBackupFiles() (_ <-chan string, _ <-chan error) {
	fileCh, errCh := streamFilesFromS3(f.client, f.s3Config)
	filterFileCh := make(chan string)

	go func() {
		defer close(filterFileCh)

		for file := range fileCh {
			if err := f.decoder.Validate(file); err != nil {
				continue
			}
			filterFileCh <- file
		}
	}()

	return filterFileCh, errCh
}

func streamFilesFromS3(client *s3.Client, s3Config *StorageConfig) (_ <-chan string, _ <-chan error) {
	fileCh := make(chan string)
	errCh := make(chan error)

	go func() {
		defer close(fileCh)
		defer close(errCh)

		var continuationToken *string

		for {
			prefix := strings.Trim(s3Config.Prefix, "/") + "/"
			listResponse, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
				Bucket:            &s3Config.Bucket,
				Prefix:            &prefix,
				ContinuationToken: continuationToken,
			})

			if err != nil {
				errCh <- err
				return
			}

			for _, p := range listResponse.Contents {
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

type s3Reader struct {
	reader io.Reader
	closer io.Closer
	closed bool
}

var _ io.ReadCloser = (*s3Reader)(nil)

func (f *s3ReaderFactory) newS3Reader(key string) (io.ReadCloser, error) {
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

	return &s3Reader{
		reader: bufio.NewReaderSize(getObjectOutput.Body, chunkSize),
		closer: getObjectOutput.Body,
	}, nil
}

func (r *s3Reader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, os.ErrClosed
	}

	return r.reader.Read(p)
}

func (r *s3Reader) Close() error {
	if r.closed {
		return os.ErrClosed
	}

	r.closed = true

	return r.closer.Close()
}

func (f *s3ReaderFactory) GetType() string {
	return s3type
}
