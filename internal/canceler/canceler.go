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

package canceler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Context canceler accumulate cancel functions to cancel them at the end of execution.
type Context struct {
	mu         sync.Mutex
	cancelFunc []context.CancelFunc
	timeout    time.Duration
	disabled   atomic.Bool
}

// NewContext returns new Context Canceler
func NewContext() *Context {
	return &Context{
		cancelFunc: make([]context.CancelFunc, 0),
	}
}

// AddCancelFunc adds cancel func for postponed call.
func (c *Context) AddCancelFunc(cancelFunc context.CancelFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancelFunc = append(c.cancelFunc, cancelFunc)
}

// Cancel run all cancel functions.
func (c *Context) Cancel() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, cancelFunc := range c.cancelFunc {
		cancelFunc()
	}
}

func (c *Context) Observe(timeout time.Duration, cancelFunc context.CancelFunc) {
	time.Sleep(timeout)
	ticker := time.NewTicker(timeout)
	defer ticker.Stop()

}

func (c *Context) DisableTimeout() {

}
