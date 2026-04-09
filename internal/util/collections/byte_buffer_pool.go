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

package collections

import (
	"bytes"
	"log/slog"
	"sync"
)

// ByteBufferPool is pool for reusable *bytes.Buffer.
type ByteBufferPool struct {
	pool sync.Pool // stores *bytes.Buffer
}

// NewByteBufferPool creates a new pool.
func NewByteBufferPool(size int) *ByteBufferPool {
	p := &ByteBufferPool{}
	p.pool.New = func() any {
		slog.Warn("Creating new byte buffer pool")
		return bytes.NewBuffer(make([]byte, 0, size))
	}

	return p
}

// Get retrieves a buffer from the pool or allocates one.
// The buffer is always reset to an empty state.
func (p *ByteBufferPool) Get() *bytes.Buffer {
	slog.Warn("Getting byte buffer pool")
	b := p.pool.Get().(*bytes.Buffer)
	b.Reset()

	return b
}

// Put returns a buffer to the pool.
func (p *ByteBufferPool) Put(b *bytes.Buffer) {
	p.pool.Put(b)
}
