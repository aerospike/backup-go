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
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"

	"github.com/aerospike/backup-go/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// netErrors contains errors to retry on. All errors should be in lower case.
var netErrors = []string{
	"connection reset",
	"broken pipe",
	"timeout",
	"connection refused",
	"no route to host",
	"i/o timeout",
	"connection timed out",
	"network is unreachable",
	"unexpected eof",
}

type s3getter interface {
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options),
	) (*s3.HeadObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options),
	) (*s3.GetObjectOutput, error)
}

// readerCloser interface for mocking tests.
//
//nolint:unused // Used only for tests.
type readerCloser interface {
	io.ReadCloser
}

// retryableReader is a wrapper around the s3 reader that implements the retryable interface.
type retryableReader struct {
	client      s3getter
	retryPolicy *models.RetryPolicy

	ctx    context.Context
	reader io.ReadCloser
	closed atomic.Bool

	bucket    string
	key       string
	position  int64
	totalSize int64

	logger *slog.Logger
}

// newRetryableReader returns a new retryable reader.
func newRetryableReader(
	ctx context.Context, client s3getter, retryPolicy *models.RetryPolicy, logger *slog.Logger, bucket, key string,
) (*retryableReader, error) {
	// Get file size to calculate when to finish.
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object size: %w", err)
	}

	r := &retryableReader{
		client:      client,
		bucket:      bucket,
		key:         key,
		totalSize:   *head.ContentLength,
		retryPolicy: retryPolicy,
		ctx:         ctx,
		logger:      logger,
	}

	if err := r.openStream(); err != nil {
		return nil, fmt.Errorf("failed to open initial object stream: %w", err)
	}

	if logger != nil {
		logger.Debug("created retryable reader",
			slog.String("bucket", bucket),
			slog.String("key", key),
			slog.Any("retryPolicy", retryPolicy),
		)
	}

	return r, nil
}

// openStream opens a new stream with offset if needed.
func (r *retryableReader) openStream() error {
	if r.closed.Load() {
		return fmt.Errorf("reader is closed")
	}

	// Calc the range header if we need to resume the download.
	var rangeHeader *string

	if r.position > 0 {
		// We read from the current position till the end of the file.
		// Check https://www.rfc-editor.org/rfc/rfc9110.html#name-byte-ranges for more details.
		rh := fmt.Sprintf("bytes=%d-", r.position)
		if r.logger != nil {
			r.logger.Debug("start reading from", slog.String("position", rh))
		}

		rangeHeader = &rh
	}

	resp, err := r.client.GetObject(r.ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(r.key),
		Range:  rangeHeader,
	})

	if err != nil {
		return fmt.Errorf("failed to get object: %w", err)
	}

	// Close previous stream if exists.
	if r.reader != nil {
		err = r.reader.Close()
		// Ignore error.
		if err != nil && r.logger != nil {
			r.logger.Error("failed to close previous stream", slog.Any("err", err))
		}
	}

	// Set a new stream.
	r.reader = resp.Body

	return nil
}

// Read reads from the stream.
func (r *retryableReader) Read(p []byte) (int, error) {
	if r.closed.Load() {
		return 0, fmt.Errorf("reader is closed")
	}
	// If we reached end of file, return EOF.
	if r.position >= r.totalSize {
		return 0, io.EOF
	}

	var (
		lastErr error
		attempt uint
	)

	for r.retryPolicy.AttemptsLeft(attempt) {
		n, err := r.reader.Read(p)
		if err == nil {
			// Success reading updated position.
			r.position += int64(n)

			return n, err
		}
		// To return the last error at the end of execution.
		lastErr = err

		if r.logger != nil {
			r.logger.Debug("got retryable reader error", slog.Any("err", err))
		}

		if isNetworkError(err) {
			if r.logger != nil {
				r.logger.Debug("retry read", slog.Any("attempt", attempt), slog.Any("err", err))
			}

			r.retryPolicy.Sleep(attempt)

			attempt++

			// Open a new stream.
			if rErr := r.openStream(); rErr != nil {
				return n, fmt.Errorf("failed to reopen stream after %d attempts: %w", attempt, rErr)
			}

			continue
		}

		return n, err
	}

	return 0, fmt.Errorf("failed after %d attempts: %w", attempt-1, lastErr)
}

// Close closes the reader.
func (r *retryableReader) Close() error {
	if r.closed.Load() {
		return nil
	}

	r.closed.Store(true)

	if r.reader != nil {
		return r.reader.Close()
	}

	return nil
}

func isNetworkError(err error) bool {
	if err == nil {
		return false
	}

	// Check error string.
	errStr := strings.ToLower(err.Error())

	for _, netErr := range netErrors {
		if strings.Contains(errStr, netErr) {
			return true
		}
	}

	return false
}
