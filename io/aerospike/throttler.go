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
	"time"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// ThrottleLimiter notification mechanism that manages active connections.
// On success, you should call Notify, on error, you should call Wait and restart the operation.
// So throttler will wait for the next available slot.
type ThrottleLimiter struct {
	slots chan struct{}
	// Timeout when Wait will return. Not to block forever.
	timeout time.Duration
}

// NewThrottleLimiter creates a new ThrottleLimiter.
func NewThrottleLimiter(timeout time.Duration) *ThrottleLimiter {
	return &ThrottleLimiter{
		slots:   make(chan struct{}),
		timeout: timeout,
	}
}

// Notify is called when a scan is done, success or error.
// It notifies one or all waiting readers to try and take the freed slot.
func (t *ThrottleLimiter) Notify(ctx context.Context) {
	select {
	case t.slots <- struct{}{}:
	case <-ctx.Done():
	}
}

// Wait blocks until someone calls Notify OR the timeout hits.
func (t *ThrottleLimiter) Wait(ctx context.Context) {
	timer := time.NewTimer(t.timeout + jitterDuration())
	defer timer.Stop()

	// Blocking everything until smth happens.
	select {
	case <-ctx.Done():
		// Context done
	case <-t.slots:
		// A slot just opened.
	case <-timer.C:
		// Timer for the situation, when the slot is opened by itself.
	}
}

// jitterDuration adds a random duration to the timeout, not to restart everything at the same moment.
func jitterDuration() time.Duration {
	const (
		minMs = 1000
		maxMs = 2000
	)

	// Range size (inclusive)
	rangeSize := maxMs - minMs + 1

	n, _ := rand.Int(rand.Reader, big.NewInt(int64(rangeSize)))

	ms := minMs + int(n.Int64())

	return time.Duration(ms) * time.Millisecond
}

// shouldThrottle determines if we should throttle connections.
func shouldThrottle(err error) bool {
	if err == nil {
		return false
	}

	var ae a.Error
	if errors.As(err, &ae) {
		return ae.Matches(types.NO_AVAILABLE_CONNECTIONS_TO_NODE) || ae.Matches(types.FAIL_FORBIDDEN)
	}

	// Additional errors that should be throttled.
	return errors.Is(err, a.ErrConnectionPoolEmpty) ||
		errors.Is(err, a.ErrConnectionPoolExhausted) ||
		errors.Is(err, a.ErrTooManyConnectionsForNode)
}
