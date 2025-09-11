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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"syscall"

	"github.com/aerospike/backup-go/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

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

	eTag      *string
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

	// Set the default retry policy if it is not set.
	if retryPolicy == nil {
		retryPolicy = models.NewDefaultRetryPolicy()
	}

	if logger != nil {
		logger = logger.With(
			slog.String("bucket", bucket),
			slog.String("key", key),
		)
	}

	r := &retryableReader{
		client:      client,
		bucket:      bucket,
		key:         key,
		retryPolicy: retryPolicy,
		ctx:         ctx,
		logger:      logger,
		eTag:        head.ETag,
	}

	if head.ContentLength != nil {
		r.totalSize = *head.ContentLength
	}

	if err := r.openStream(); err != nil {
		return nil, fmt.Errorf("failed to open initial object stream: %w", err)
	}

	if logger != nil {
		logger.Debug("created retryable reader",
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
			r.logger.Debug("start reading from",
				slog.String("position", rh),
			)
		}

		rangeHeader = &rh
	}

	resp, err := r.client.GetObject(r.ctx, &s3.GetObjectInput{
		Bucket:  aws.String(r.bucket),
		Key:     aws.String(r.key),
		Range:   rangeHeader,
		IfMatch: r.eTag,
	})

	if err != nil {
		return fmt.Errorf("failed to get object: %w", err)
	}

	// Close previous stream if exists.
	if r.reader != nil {
		err = r.reader.Close()
		// Log error, as it is not critical, doesn't interrupt the process.
		if err != nil && r.logger != nil {
			r.logger.Error("failed to close previous stream",
				slog.Any("err", err),
			)
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
		// Do not log EOF errors.
		if r.logger != nil && !errors.Is(err, io.EOF) {
			r.logger.Debug("retryable reader got error",
				slog.Any("err", err),
			)
		}

		if isNetworkError(err) {
			if r.logger != nil {
				r.logger.Debug("retry read",
					slog.Any("attempt", attempt),
					slog.Any("err", err),
				)
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

	return 0, fmt.Errorf("failed after %d attempts: %w", attempt, lastErr)
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

	if errors.Is(err, syscall.ECONNRESET) || // "connection reset"
		errors.Is(err, syscall.EPIPE) || // "broken pipe"
		errors.Is(err, syscall.ETIMEDOUT) || // "timeout"
		errors.Is(err, syscall.ECONNREFUSED) || // "connection refused"
		errors.Is(err, syscall.ENETUNREACH) || // "network is unreachable"
		errors.Is(err, syscall.ECONNABORTED) || // "software caused connection abort"
		errors.Is(err, syscall.EHOSTUNREACH) || // "no route to host"
		errors.Is(err, io.ErrClosedPipe) || // "closed pipe"
		errors.Is(err, io.ErrUnexpectedEOF) { // "unexpected eof"
		return true
	}

	// For timeouts surfaced as net.Error (e.g. "i/o timeout")
	var nErr net.Error
	if errors.As(err, &nErr) && nErr.Timeout() {
		return true
	}

	return false
}
