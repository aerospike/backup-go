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

package lazy

import (
	"context"
	"fmt"
	"io"
)

// Writer wraps an io.WriteCloser and creates a writer only on Write operation.
type Writer struct {
	ctx    context.Context // stored internally to be used by the Write method
	writer io.WriteCloser
	open   func(context.Context, string) (io.WriteCloser, error)
	n      int
}

// NewWriter creates a new lazy Writer.
func NewWriter(ctx context.Context,
	n int,
	open func(context.Context, string) (io.WriteCloser, error),
) (*Writer, error) {
	return &Writer{
		ctx:  ctx,
		open: open,
		n:    n,
	}, nil
}

func (f *Writer) Write(p []byte) (n int, err error) {
	if f.writer == nil {
		f.writer, err = f.open(f.ctx, fmt.Sprintf("%d_", f.n))
		if err != nil {
			return 0, fmt.Errorf("failed to open writer: %w", err)
		}
	}

	n, err = f.writer.Write(p)

	return n, err
}

func (f *Writer) Close() error {
	if f.writer == nil { // in case there were no writes
		return nil
	}

	return f.writer.Close()
}
