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
	"io"
	"os"

	ioStorage "github.com/aerospike/backup-go/io/storage"
)

const (
	bufferSize = 4096 * 1024 // 4mb
	stdoutType = "stdout"
)

// Writer implements stdout writer.
type Writer struct{}

// NewWriter creates a new stdout writer. opts are ignored.
func NewWriter(ctx context.Context, _ ...ioStorage.Opt) (*Writer, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return &Writer{}, nil
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
// The file name is ignored for stdout.
func (w *Writer) NewWriter(ctx context.Context, _ string) (io.WriteCloser, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return &stdoutWriteCloser{
		Writer: bufio.NewWriterSize(os.Stdout, bufferSize),
	}, nil
}

// RemoveFiles no-op for stdout.
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
