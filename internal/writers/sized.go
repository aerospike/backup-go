// Copyright 2024-2024 Aerospike, Inc.
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

package writers

import (
	"fmt"
	"io"
	"log/slog"
)

// Sized wraps an io.WriteCloser and adds a size limit.
// when the size limit is reached, the io.WriteCloser is closed and a new one is created
// using the open function.
type Sized struct {
	writer io.WriteCloser
	open   func() (io.WriteCloser, error)
	size   int64
	limit  int64
}

// NewSized creates a new Sized writer with a size limit.
// limit must be greater than 0.
func NewSized(limit int64, open func() (io.WriteCloser, error)) (*Sized, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be greater than 0, got %d", limit)
	}

	return &Sized{
		limit: limit,
		open:  open,
	}, nil
}

func (f *Sized) Write(p []byte) (n int, err error) {
	if f.size >= f.limit {
		err := f.writer.Close()
		if err != nil {
			return 0, err
		}

		f.size = 0
		f.writer = nil
	}

	if f.writer == nil {
		f.writer, err = f.open()
		if err != nil {
			return 0, err
		}
	}

	n, err = f.writer.Write(p)
	f.size += int64(n)

	return n, err
}

func (f *Sized) Close() error {
	_, _ = f.Write([]byte{})
	slog.Info("Close sized")
	return f.writer.Close()
}
