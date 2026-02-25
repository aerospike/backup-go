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

package aerospike

import (
	"context"
	"crypto/rand"
	"errors"
	"math/big"
	"sync"
	"time"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// ShouldThrottle determines if we should throttler connection.
func ShouldThrottle(err error) bool {
	if err == nil {
		return false
	}

	var ae a.Error
	if errors.As(err, &ae) {
		return ae.Matches(types.NO_AVAILABLE_CONNECTIONS_TO_NODE) || ae.Matches(types.FAIL_FORBIDDEN)
	}

	// Addititonal errors that should be throttled.
	return errors.Is(err, a.ErrConnectionPoolEmpty) ||
		errors.Is(err, a.ErrConnectionPoolExhausted) ||
		errors.Is(err, a.ErrTooManyConnectionsForNode)
}

type ThrottleLimiter struct {
	mu   sync.Mutex
	cond *sync.Cond
	// activeCount is the number of scans that are currently active.
	// We not use atomic because we need to lock this value during Wait().
	activeCount int
}

func NewThrottleLimiter() *ThrottleLimiter {
	t := &ThrottleLimiter{}
	t.cond = sync.NewCond(&t.mu)

	return t
}

// Started is called when a scan successfully receives its first record.
func (t *ThrottleLimiter) Started() {
	t.mu.Lock()

	t.activeCount++

	t.mu.Unlock()
}

// Finished is called when a scan is done, success or error.
// It notifies one or all waiting readers to try and take the freed slot.
func (t *ThrottleLimiter) Finished() {
	t.mu.Lock()

	if t.activeCount > 0 {
		t.activeCount--
	}

	t.mu.Unlock()

	// Wake up everyone to race for the new free slot
	t.cond.Broadcast()
}

// Wait blocks until someone calls Finished OR the timeout hits.
func (t *ThrottleLimiter) Wait(ctx context.Context, timeout time.Duration) {
	waitDone := make(chan struct{})

	go func() {
		t.mu.Lock()
		t.cond.Wait()
		t.mu.Unlock()
		close(waitDone)
	}()

	select {
	case <-ctx.Done():
		// Context done
	case <-waitDone:
		// A slot just opened up!
	case <-time.After(timeout):
		// TODO: may be use time.NewTimer(timeout) here?

		// No scan finished, but let's try again anyway
		// in case external Scan finished.
	}
}

func jitterDuration() time.Duration {
	const (
		minMs = 5000
		maxMs = 8000
	)

	// Range size (inclusive)
	rangeSize := maxMs - minMs + 1

	n, _ := rand.Int(rand.Reader, big.NewInt(int64(rangeSize)))

	ms := minMs + int(n.Int64())

	return time.Duration(ms) * time.Millisecond
}
