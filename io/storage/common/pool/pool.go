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

package pool

import (
	"sync"
)

// Pool is a simple goroutine pool.
type Pool struct {
	workers  int
	wg       sync.WaitGroup
	workChan chan struct{}
}

// NewPool returns new goroutine pool.
// workers <= 0 -> nil (sync mode).
func NewPool(workers int) *Pool {
	if workers <= 0 {
		return nil
	}

	p := &Pool{
		workers:  workers,
		workChan: make(chan struct{}, workers),
	}

	return p
}

// Submit adds new goroutine to pool.
// If p == nil, runs synchronously.
func (p *Pool) Submit(f func()) {
	if p == nil {
		f()

		return
	}

	// Adding empty field to chanel to take one worker place.
	p.workChan <- struct{}{}
	// Adding one to a waiting group.
	p.wg.Add(1)

	go func() {
		// Remove message from channel to release space for new worker.
		defer func() { <-p.workChan }()
		// When function will finish it's work we release a waiting group.
		defer p.wg.Done()
		// Run our goroutine.
		f()
	}()
}

// Wait waits until all submitted funcs complete.
// No-op for nil pool.
func (p *Pool) Wait() {
	if p == nil {
		return
	}

	p.wg.Wait()
}
