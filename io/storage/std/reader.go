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
	"path/filepath"

	"github.com/aerospike/backup-go/io/storage/common"
	"github.com/aerospike/backup-go/models"
)

const stdinType = "stdin"

// Reader represents an stdin reader.
type Reader struct {
	bufferSize int
}

// NewReader creates a new stdin reader.
func NewReader(ctx context.Context, bufferSize int) (*Reader, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if bufferSize < 0 {
		return nil, fmt.Errorf("buffer size must not be negative")
	}

	if bufferSize == 0 {
		bufferSize = defaultBufferSize
	}

	return &Reader{bufferSize: bufferSize}, nil
}

// GetType returns the `stdType` type of storage. Used in logging.
func (r *Reader) GetType() string {
	return stdinType
}

// GetSize returns -1 for stdin, indicating that estimates should not be calculated.
func (r *Reader) GetSize() int64 {
	// No need to wait calculations for stdin.
	return -1
}

// GetNumber returns -1 for stdin, indicating that estimates should not be calculated.
func (r *Reader) GetNumber() int64 {
	// No need to wait calculations for stdin.
	return -1
}

// ListObjects returns a list of objects in the path.
// Always returns one element "stdin".
func (r *Reader) ListObjects(ctx context.Context, _ string) ([]string, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return []string{stdinType}, nil
}

// StreamFile opens stdin as a file and sends io.Readers to the `readersCh`
func (r *Reader) StreamFile(
	ctx context.Context, filename string, readersCh chan<- models.File, errorsCh chan<- error,
) {
	if ctx.Err() != nil {
		common.ErrToChan(ctx, errorsCh, ctx.Err())
		return
	}

	readCloser := io.NopCloser(bufio.NewReaderSize(os.Stdin, r.bufferSize))

	readersCh <- models.File{Reader: readCloser, Name: filepath.Base(filename)}
}

// StreamFiles opens stdin as files and sends io.Readers to the `readersCh`
func (r *Reader) StreamFiles(
	ctx context.Context, readersCh chan<- models.File, errorsCh chan<- error, _ []string,
) {
	defer close(readersCh)

	r.StreamFile(ctx, stdinType, readersCh, errorsCh)
}

// GetSkipped returns a list of file paths that were skipped during the `StreamFlies` with skipPrefix.
// no-op func to satisfy interface.
func (r *Reader) GetSkipped() []string {
	return nil
}
