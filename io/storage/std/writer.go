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

package std

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
)

const (
	defaultBufferSize = 4096 * 1024 // 4mb
	stdoutType        = "stdout"
)

// Writer represents an stdout writer.
type Writer struct {
	bufferSize int
}

// NewWriter creates a new stdout writer.
func NewWriter(ctx context.Context, bufferSize int) (*Writer, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if bufferSize < 0 {
		return nil, fmt.Errorf("buffer size must not be negative")
	}

	if bufferSize == 0 {
		bufferSize = defaultBufferSize
	}

	return &Writer{bufferSize: bufferSize}, nil
}

// stdoutWriteCloser implements io.WriteCloser for stdout.
type stdoutWriteCloser struct {
	*bufio.Writer
}

// Close flushes stdout.
func (w *stdoutWriteCloser) Close() error {
	return w.Flush()
}

// NewWriter creates a new stdout writer closer.
// The file name and isMeta is ignored for stdout.
func (w *Writer) NewWriter(ctx context.Context, _ string, _ bool) (io.WriteCloser, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return &stdoutWriteCloser{
		Writer: bufio.NewWriterSize(os.Stdout, w.bufferSize),
	}, nil
}

// RemoveFiles is a no-op for stdout.
func (w *Writer) RemoveFiles(_ context.Context) error {
	return nil
}

// Remove no-op for stdout.
func (w *Writer) Remove(_ context.Context, _ string) error {
	return nil
}

// GetType returns the `stdType` type of storage. Used in logging.
func (w *Writer) GetType() string {
	return stdoutType
}
