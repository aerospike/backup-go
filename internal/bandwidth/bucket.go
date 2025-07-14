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
	"fmt"
	"sync"
	"time"
)

// Bucket implements a thread-safe leaky bucket rate limiter.
type Bucket struct {
	// mu to make bucket thread safe.
	// As bucket is used for limiting speed, one mutex won't affect speed.
	mu sync.Mutex

	// Maximum tokens in the bucket.
	limit int64

	// Rate at which tokens are added (tokens per nanosecond).
	rate float64

	// Current available tokens (can be fractional for precise calculations).
	tokens float64

	// Last time we leaked tokens.
	lastLeak time.Time
}

// NewBucket creates a new rate limiter with the specified limit and interval.
func NewBucket(limit int64, interval time.Duration) (*Bucket, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be greater than 0")
	}

	if interval <= 0 {
		return nil, fmt.Errorf("interval must be greater than 0")
	}
	// Calculate rate as tokens per nanosecond for precise calculations.
	rate := float64(limit) / float64(interval.Nanoseconds())

	return &Bucket{
		limit:    limit,
		rate:     rate,
		tokens:   float64(limit),
		lastLeak: time.Now(),
	}, nil
}

// Wait blocks until n tokens are available.
// It allows waiting for amounts larger than the limit.
func (rl *Bucket) Wait(n int64) {
	if n < 1 {
		return
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Leak tokens.
	rl.leak()

	// If we have enough tokens, use them and return.
	if rl.tokens >= float64(n) {
		rl.tokens -= float64(n)
		return
	}

	// Calculate exact time needed to accumulate required tokens.
	tokensNeeded := float64(n) - rl.tokens
	waitDuration := time.Duration(tokensNeeded / rl.rate)

	// Wait for exact duration.
	time.Sleep(waitDuration)

	// After waiting, leak tokens again and consume.
	rl.leak()
	rl.tokens -= float64(n)
}

// leak updates the token count based on elapsed time.
func (rl *Bucket) leak() {
	now := time.Now()
	elapsed := now.Sub(rl.lastLeak)

	// Calculate exact tokens to add based on elapsed time.
	tokensToAdd := rl.rate * float64(elapsed.Nanoseconds())

	if tokensToAdd > 0 {
		rl.tokens += tokensToAdd

		// Don't overflow the bucket.
		if rl.tokens > float64(rl.limit) {
			rl.tokens = float64(rl.limit)
		}

		// Update last leak time to current time.
		rl.lastLeak = now
	}
}
