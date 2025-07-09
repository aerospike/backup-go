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

package bandwidth

import (
	"sync"
	"time"
)

// Bucket implements a thread-safe leaky bucket rate limiter
type Bucket struct {
	// mu to make bucket thread safe.
	// As bucket is used for limiting speed, one mutex won't affect speed.
	mu sync.Mutex

	// Maximum tokens in the bucket
	limit int64

	// Time interval for refilling the bucket
	interval time.Duration

	// Current available tokens
	tokens int64

	// Last time we leaked tokens
	lastLeak time.Time
}

// NewBucket creates a new rate limiter with the specified limit and interval
func NewBucket(limit int64, interval time.Duration) *Bucket {
	return &Bucket{
		limit:    limit,
		interval: interval,
		tokens:   limit,
		lastLeak: time.Now(),
	}
}

// Wait blocks until n tokens are available
// It allows waiting for amounts larger than the limit
func (rl *Bucket) Wait(n int64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Leak tokens.
	rl.leak()

	// If we have enough tokens, use them and return.
	if rl.tokens >= n {
		rl.tokens -= n
		return
	}

	// If we don't have enough tokens, calculate the waiting time.
	tokensNeeded := n - rl.tokens

	// Calculate how many full intervals we need to wait.
	// Each interval adds `limit` tokens.
	intervalsToWait := tokensNeeded / rl.limit

	// If we need a partial interval, add one more.
	if tokensNeeded%rl.limit > 0 {
		intervalsToWait++
	}

	// Calculate the exact wait duration.
	totalWait := time.Duration(intervalsToWait) * rl.interval

	// Wait.
	time.Sleep(totalWait)

	// After waiting, leak tokens again.
	rl.leak()
	rl.tokens -= n
}

// leak updates the token count, according to leak.
func (rl *Bucket) leak() {
	now := time.Now()
	elapsed := now.Sub(rl.lastLeak)

	// Calculate how many full intervals have passed.
	intervals := int64(elapsed / rl.interval)

	if intervals > 0 {
		// Add tokens for each full interval.
		rl.tokens += intervals * rl.limit

		// Don't overflow the bucket.
		if rl.tokens > rl.limit {
			rl.tokens = rl.limit
		}

		// Update last leak time to the start of the current interval.
		rl.lastLeak = rl.lastLeak.Add(time.Duration(intervals) * rl.interval)
	}
}
