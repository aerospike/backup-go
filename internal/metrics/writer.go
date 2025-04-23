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

package metrics

import (
	"io"
)

// Writer wraps an io.Writer to collect metrics on write operations.
type Writer struct {
	writer    io.WriteCloser
	collector *Collector
}

// NewWriter creates a new metrics wrapper around an existing io.Writer.
func NewWriter(w io.WriteCloser, c *Collector) *Writer {
	return &Writer{
		writer:    w,
		collector: c,
	}
}

func (w *Writer) Write(p []byte) (n int, err error) {
	n, err = w.writer.Write(p)

	w.collector.Add(uint64(n))

	return n, err
}

func (w *Writer) Close() error {
	return w.writer.Close()
}
