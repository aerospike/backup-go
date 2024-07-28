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
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3StreamingReader struct {
	client   *s3.Client
	s3Config *StorageConfig
	validate func(string) error
}

var _ backup.StreamingReader = (*s3StreamingReader)(nil)

var ErrRestoreDirectoryInvalid = errors.New("restore directory is invalid")

func NewS3StreamingReader(
	ctx context.Context, config *StorageConfig, validate func(string) error,
) (backup.StreamingReader, error) {
	if validate == nil {
		return nil, fmt.Errorf("validation function is required")
	}

	client, err := newS3Client(ctx, config)
	if err != nil {
		return nil, err
	}

	return &s3StreamingReader{
		client:   client,
		s3Config: config,
		validate: validate,
	}, nil
}

// StreamFiles read files form s3 and send io.Readers to `readersCh` communication
// chan for lazy loading.
// In case of error we send error to `errorsCh` channel.
func (f *s3StreamingReader) StreamFiles(
	ctx context.Context, readersCh chan<- io.ReadCloser, errorsCh chan<- error,
) {
	fileCh, s3errCh := f.streamBackupFiles(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case file, ok := <-fileCh:
			if !ok {
				fileCh = nil
			} else {
				reader, err := f.newS3Reader(ctx, file)
				if err != nil {
					errorsCh <- err
					return
				}

				readersCh <- reader
			}

		case err, ok := <-s3errCh:
			if ok {
				errorsCh <- err
				return
			}

			s3errCh = nil
		}

		if fileCh == nil && s3errCh == nil {
			break
		}
	}

	close(readersCh)
}

func (f *s3StreamingReader) streamBackupFiles(
	ctx context.Context,
) (_ <-chan string, _ <-chan error) {
	fileCh, errCh := streamFilesFromS3(ctx, f.client, f.s3Config)
	filterFileCh := make(chan string)

	go func() {
		defer close(filterFileCh)

		for file := range fileCh {
			if err := f.validate(file); err != nil {
				continue
			}
			filterFileCh <- file
		}
	}()

	return filterFileCh, errCh
}

func streamFilesFromS3(
	ctx context.Context, client *s3.Client, s3Config *StorageConfig,
) (_ <-chan string, _ <-chan error) {
	fileCh := make(chan string)
	errCh := make(chan error)

	go func() {
		defer close(fileCh)
		defer close(errCh)

		var continuationToken *string

		for {
			prefix := strings.Trim(s3Config.Prefix, "/") + "/"
			listResponse, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
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

func (f *s3StreamingReader) newS3Reader(ctx context.Context, key string) (io.ReadCloser, error) {
	getObjectOutput, err := f.client.GetObject(ctx, &s3.GetObjectInput{
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

func (f *s3StreamingReader) GetType() string {
	return s3type
}
