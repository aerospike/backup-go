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

package backup

import (
	"context"
	"sync"
)

// handlerBase contains common fields and methods for backup/restore handlers.
type handlerBase struct {
	// Global context for the operation.
	ctx    context.Context
	cancel context.CancelFunc

	// Channel for errors from the operation.
	errors chan error
	// Channel signaling successful completion.
	done chan struct{}

	// For graceful shutdown.
	wg sync.WaitGroup
}

// newHandlerBase creates a new handlerBase with the given context.
func newHandlerBase(ctx context.Context) *handlerBase {
	ctx, cancel := context.WithCancel(ctx)

	return &handlerBase{
		ctx:    ctx,
		cancel: cancel,
		errors: make(chan error, 1),
		done:   make(chan struct{}, 1),
	}
}

// waitForCompletion waits for the operation to complete and returns any error.
// It ensures cancel is called in all paths to prevent goroutine leaks.
func (h *handlerBase) waitForCompletion(waitCtx context.Context) error {
	var err error

	select {
	case <-h.ctx.Done():
		// Global context is done.
		err = h.ctx.Err()
		// Always cancel to stop all goroutines and prevent leaks.
		h.cancel()
	case <-waitCtx.Done():
		// Wait context is done.
		err = waitCtx.Err()
		// Always cancel to stop all goroutines and prevent leaks.
		h.cancel()
	case err = <-h.errors:
		// Operation failed.
		// Always cancel to stop all goroutines and prevent leaks.
		h.cancel()
	case <-h.done: // Success.
	}

	// Wait for all goroutines to finish.
	h.wg.Wait()

	return err
}
