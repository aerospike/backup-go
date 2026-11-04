// Copyright 2024-2026 Aerospike, Inc.
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
	"sync/atomic"
)

// Reader wraps an io.Reader to collect metrics on read operations.
type Reader struct {
	reader    io.ReadCloser
	collector *Collector
}

// NewReader creates a new metrics wrapper around an existing io.Reader.
func NewReader(r io.ReadCloser, c *Collector) *Reader {
	return &Reader{
		reader:    r,
		collector: c,
	}
}

// Read reads data from the reader and collects metrics on the number of bytes read.
func (r *Reader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)

	r.collector.Add(uint64(n))

	return n, err
}

// Close closes the reader.
func (r *Reader) Close() error {
	return r.reader.Close()
}

// CountingReader wraps an io.ReadCloser and adds the number of bytes read to a counter.
type CountingReader struct {
	reader  io.ReadCloser
	counter *atomic.Uint64
}

// NewCountingReader creates a new CountingReader around an existing io.ReadCloser.
func NewCountingReader(r io.ReadCloser, counter *atomic.Uint64) *CountingReader {
	return &CountingReader{
		reader:  r,
		counter: counter,
	}
}

// Read reads data from the reader and adds the number of bytes read to the counter.
func (r *CountingReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)

	if r.counter != nil && n > 0 {
		r.counter.Add(uint64(n))
	}

	return n, err
}

// Close closes the reader.
func (r *CountingReader) Close() error {
	return r.reader.Close()
}
