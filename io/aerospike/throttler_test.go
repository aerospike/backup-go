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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestThrottleLimiter_NotifyUnblocksWait(t *testing.T) {
	// Use a small timeout to keep tests fast
	timeout := 10 * time.Second
	tl := NewThrottleLimiter(2, timeout)
	ctx := t.Context()

	var wokenUp atomic.Int32
	go func() {
		tl.Wait(ctx)
		wokenUp.Add(1)
	}()

	// Give the goroutine a moment to enter Wait
	time.Sleep(100 * time.Millisecond)

	if wokenUp.Load() != 0 {
		t.Error("Wait should have blocked")
	}

	tl.Notify(ctx)

	// Check if it woke up (using a small retry loop or timeout)
	time.Sleep(100 * time.Millisecond)
	if wokenUp.Load() != 1 {
		t.Error("Wait should have been unblocked by Notify")
	}
}

func TestThrottleLimiter_OneNotifyPerWait(t *testing.T) {
	tl := NewThrottleLimiter(5, 10*time.Second)
	ctx := t.Context()

	var wokenCount atomic.Int32
	wg := sync.WaitGroup{}

	// Start 3 waiters
	for range 3 {
		wg.Add(1)
		wg.Go(func() {
			defer wg.Done()
			tl.Wait(ctx)
			wokenCount.Add(1)
		})
	}

	time.Sleep(100 * time.Millisecond)

	// First Notify
	tl.Notify(ctx)
	time.Sleep(100 * time.Millisecond)
	if wokenCount.Load() != 1 {
		t.Errorf("Expected 1 woken waiter, got %d", wokenCount.Load())
	}

	// Second Notify
	tl.Notify(ctx)
	time.Sleep(100 * time.Millisecond)
	if wokenCount.Load() != 2 {
		t.Errorf("Expected 2 woken waiters, got %d", wokenCount.Load())
	}
}

func TestThrottleLimiter_TimeoutFallback(t *testing.T) {
	// Use very short timeout for test
	shortTimeout := 100 * time.Millisecond
	tl := NewThrottleLimiter(2, shortTimeout)
	ctx := t.Context()

	start := time.Now()
	tl.Wait(ctx)
	duration := time.Since(start)

	// jitter adds 1-2s, so we expect duration > 1.1s
	if duration < 1*time.Second {
		t.Errorf("Wait returned too early: %v", duration)
	}
}

func TestThrottleLimiter_ContextCancel(t *testing.T) {
	tl := NewThrottleLimiter(2, 10*time.Second)
	ctx, cancel := context.WithCancel(t.Context())

	start := time.Now()
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	tl.Wait(ctx)
	duration := time.Since(start)

	if duration > 1*time.Second {
		t.Errorf("Wait should have returned immediately on context cancel, took %v", duration)
	}
}
