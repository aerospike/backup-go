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

package sized

import (
	"context"
	"fmt"
	"io"
)

// Writer wraps an io.WriteCloser and adds a size limit.
// when the size limit is reached, the io.WriteCloser is closed and a new one is created
// using the open function.
type Writer struct {
	ctx    context.Context // stored internally to be used by the Write method
	writer io.WriteCloser
	open   func(context.Context) (io.WriteCloser, error)
	size   int64
	limit  int64
	// Number of writer, for saving state.
	n               int
	saveCommandChan chan int
}

// NewWriter creates a new Writer with a size limit.
// limit must be greater than 0.
func NewWriter(ctx context.Context, n int, saveCommandChan chan int, limit int64,
	open func(context.Context) (io.WriteCloser, error)) (*Writer, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be greater than 0, got %d", limit)
	}

	return &Writer{
		ctx:             ctx,
		limit:           limit,
		open:            open,
		n:               n,
		saveCommandChan: saveCommandChan,
	}, nil
}

func (f *Writer) Write(p []byte) (n int, err error) {
	if f.size >= f.limit {
		err = f.writer.Close()
		if err != nil {
			return 0, fmt.Errorf("failed to close writer: %w", err)
		}

		if f.saveCommandChan != nil {
			f.saveCommandChan <- f.n
		}

		f.size = 0
		f.writer = nil
	}

	if f.writer == nil {
		f.writer, err = f.open(f.ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to open writer: %w", err)
		}
	}

	n, err = f.writer.Write(p)
	f.size += int64(n)

	return n, err
}

func (f *Writer) Close() error {
	if f.writer == nil { // in case there were no writes
		return nil
	}

	return f.writer.Close()
}
