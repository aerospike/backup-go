// Copyright 2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type validator interface {
	Run(fileName string) error
}

type StreamingReader struct {
	client    *s3.Client
	s3Config  *Config
	validator validator
}

func NewStreamingReader(
	ctx context.Context,
	s3Config *Config,
	validator validator,
) (*StreamingReader, error) {
	if validator == nil {
		return nil, fmt.Errorf("validator cannot be nil")
	}

	client, err := newS3Client(ctx, s3Config)
	if err != nil {
		return nil, err
	}

	return &StreamingReader{
		client:    client,
		s3Config:  s3Config,
		validator: validator,
	}, nil
}

// StreamFiles read files form s3 and send io.Readers to `readersCh` communication
// chan for lazy loading.
// In case of error we send error to `errorsCh` channel.
func (r *StreamingReader) StreamFiles(
	ctx context.Context, readersCh chan<- io.ReadCloser, errorsCh chan<- error,
) {
	fileCh, s3errCh := r.streamBackupFiles(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case file, ok := <-fileCh:
			if !ok {
				fileCh = nil
			} else {
				reader, err := r.newS3Reader(ctx, file)
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

func (r *StreamingReader) streamBackupFiles(
	ctx context.Context,
) (_ <-chan string, _ <-chan error) {
	fileCh, errCh := streamFilesFromS3(ctx, r.client, r.s3Config)
	filterFileCh := make(chan string)

	go func() {
		defer close(filterFileCh)

		for file := range fileCh {
			if err := r.validator.Run(file); err != nil {
				continue
			}
			filterFileCh <- file
		}
	}()

	return filterFileCh, errCh
}

func streamFilesFromS3(
	ctx context.Context,
	client *s3.Client,
	s3Config *Config,
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
				errCh <- fmt.Errorf("failed to list objects: %w", err)
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

func (r *StreamingReader) newS3Reader(ctx context.Context, key string) (io.ReadCloser, error) {
	getObjectOutput, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &r.s3Config.Bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get s3 object: %w", err)
	}

	chunkSize := r.s3Config.ChunkSize
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

// GetType return `s3type` type of storage. Used in logging.
func (r *StreamingReader) GetType() string {
	return s3type
}
