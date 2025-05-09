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

import "io"

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

func (r *Reader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)

	r.collector.Add(uint64(n))

	return n, err
}

func (r *Reader) Close() error {
	return r.reader.Close()
}
