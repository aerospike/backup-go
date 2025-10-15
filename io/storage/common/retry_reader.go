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

package common

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
)

// readerCloser interface for mocking tests.
//
//nolint:unused // Used only for tests.
type readerCloser interface {
	io.ReadCloser
}

// rangeReader interface for range reader. That reads a file from a specific offset.
type rangeReader interface {
	OpenRange(ctx context.Context, offset, count int64) (io.ReadCloser, error)
	GetSize() int64
	GetInfo() string
}

// RetryableReader is a wrapper around the s3 reader that implements the retryable interface.
type RetryableReader struct {
	ctx         context.Context
	rangeReader rangeReader
	reader      io.ReadCloser

	retryPolicy *models.RetryPolicy
	logger      *slog.Logger
	offset      int64
	totalSize   int64

	closed atomic.Bool
}

// NewRetryableReader returns a new retryable reader.
func NewRetryableReader(
	ctx context.Context,
	rangeReader rangeReader,
	retryPolicy *models.RetryPolicy,
	logger *slog.Logger,
) (*RetryableReader, error) {
	if logger != nil {
		logger = logger.With(
			slog.String("fileinfo", rangeReader.GetInfo()),
		)
	}

	// Set the default retry policy if it is not set.
	if retryPolicy == nil {
		retryPolicy = models.NewDefaultRetryPolicy()
	}

	totalSize := rangeReader.GetSize()

	r := &RetryableReader{
		ctx:         ctx,
		rangeReader: rangeReader,
		retryPolicy: retryPolicy,
		totalSize:   totalSize,
		logger:      logger,
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
func (r *RetryableReader) openStream() error {
	if r.closed.Load() {
		return fmt.Errorf("reader is closed")
	}

	if r.offset > 0 {
		if r.logger != nil {
			r.logger.Debug("start reading from",
				slog.Int64("offset", r.offset),
			)
		}
	}
	// Count = 0 means read to the end of the file.
	fReader, err := r.rangeReader.OpenRange(r.ctx, r.offset, 0)
	if err != nil {
		return fmt.Errorf("failed to get file: %w", err)
	}

	r.setReader(fReader)

	return nil
}

// setReader encapsulates set reader logic.
func (r *RetryableReader) setReader(body io.ReadCloser) {
	// Close previous stream if exists.
	if r.reader != nil {
		err := r.reader.Close()
		// Log error, as it is not critical, doesn't interrupt the process.
		if err != nil && r.logger != nil {
			r.logger.Error("failed to close previous stream",
				slog.Any("err", err),
			)
		}
	}

	// Set a new stream.
	r.reader = body
}

// Read reads from the stream.
func (r *RetryableReader) Read(p []byte) (int, error) {
	if r.closed.Load() {
		return 0, fmt.Errorf("reader is closed")
	}
	// If we reached end of file, return EOF.
	if r.offset >= r.totalSize {
		return 0, io.EOF
	}

	var (
		lastErr error
		attempt uint
	)

	for r.retryPolicy.AttemptsLeft(attempt) {
		n, err := r.reader.Read(p)
		if err == nil {
			// Success reading updated offset.
			r.offset += int64(n)

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
				r.logger.Warn("retry read",
					slog.Any("attempt", attempt),
					slog.Any("err", err),
				)
			}

			if err := r.retryPolicy.Sleep(r.ctx, attempt); err != nil {
				return 0, err
			}

			attempt++

			// Open a new stream.
			if rErr := r.openStream(); rErr != nil {
				r.logger.Warn("failed to reopen stream",
					slog.Any("attempt", attempt),
					slog.Any("err", rErr),
				)
			}

			continue
		}

		return n, err
	}

	return 0, fmt.Errorf("failed after %d attempts: %w", attempt, lastErr)
}

// Close closes the reader.
func (r *RetryableReader) Close() error {
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
		errors.Is(err, io.ErrUnexpectedEOF) || // "unexpected eof"
		errors.Is(err, context.DeadlineExceeded) { // "context deadline"
		return true
	}

	// For timeouts surfaced as net.Error (e.g. "i/o timeout")
	var nErr net.Error
	if errors.As(err, &nErr) && nErr.Timeout() {
		return true
	}

	return false
}
