package bandwidth

import (
	"sync"
	"sync/atomic"
	"time"
)

// Bucket implements a thread-safe leaky bucket rate limiter
type Bucket struct {
	mu sync.Mutex

	// Maximum tokens in the bucket
	limit         int64
	nanosPerToken int64
	// Time interval for refilling the bucket
	interval time.Duration

	// Current available tokens
	tokens int64
	// Last time we leaked tokens
	lastLeakNano int64
}

// NewBucket creates a new rate limiter with the specified limit and interval
func NewBucket(limit int64, interval time.Duration) *Bucket {
	now := time.Now().UnixNano()
	return &Bucket{
		limit:         limit,
		nanosPerToken: interval.Nanoseconds() / limit,
		interval:      interval,
		tokens:        limit,
		lastLeakNano:  now,
	}
}

// Wait blocks until n tokens are available
func (b *Bucket) Wait(n int64) {
	// Fast path: try to get tokens without slow operations
	for {
		if b.tryAcquire(n) {
			return
		}

		// Need to wait - use slow path
		b.slowWait(n)
		return
	}
}

// tryAcquire attempts to acquire n tokens using only atomic operations
func (b *Bucket) tryAcquire(n int64) bool {
	// First, try to leak tokens
	b.atomicLeak()

	// Try to consume tokens
	for {
		currentTokens := atomic.LoadInt64(&b.tokens)
		if currentTokens < n {
			return false // Not enough tokens
		}

		if atomic.CompareAndSwapInt64(&b.tokens, currentTokens, currentTokens-n) {
			return true // Success!
		}
		// CAS failed, retry
	}
}

// atomicLeak updates tokens based on elapsed time using atomic operations
func (b *Bucket) atomicLeak() {
	now := time.Now().UnixNano()

	for {
		lastLeak := atomic.LoadInt64(&b.lastLeakNano)
		elapsedNanos := now - lastLeak

		// No time passed
		if elapsedNanos <= 0 {
			return
		}

		// Less than one token worth of time
		tokensToAdd := elapsedNanos / b.nanosPerToken
		if tokensToAdd <= 0 {
			return
		}

		// Calculate new leak time
		newLeakTime := lastLeak + (tokensToAdd * b.nanosPerToken)

		// Try to update leak time first
		if !atomic.CompareAndSwapInt64(&b.lastLeakNano, lastLeak, newLeakTime) {
			continue
		}

		// Successfully updated leak time, now update tokens
		for {
			currentTokens := atomic.LoadInt64(&b.tokens)
			newTokens := currentTokens + tokensToAdd
			if newTokens > b.limit {
				newTokens = b.limit
			}

			if atomic.CompareAndSwapInt64(&b.tokens, currentTokens, newTokens) {
				// ok
				return
			}
		}
	}
}

// slowWait handles cases where we need to actually wait
func (b *Bucket) slowWait(n int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Double-check after acquiring lock
	if b.tryAcquire(n) {
		return
	}

	// Calculate how long we need to wait
	currentTokens := atomic.LoadInt64(&b.tokens)
	tokensNeeded := n - currentTokens

	// For big requests (larger than limit), calculate intervals needed
	if n > b.limit {
		intervalsToWait := tokensNeeded / b.limit
		if tokensNeeded%b.limit > 0 {
			intervalsToWait++
		}
		waitTime := time.Duration(intervalsToWait) * b.interval

		time.Sleep(waitTime)

		// After sleep, set the exact state
		now := time.Now().UnixNano()
		atomic.StoreInt64(&b.lastLeakNano, now)
		atomic.StoreInt64(&b.tokens, b.limit-n)
		return
	}

	// For normal requests, wait for needed tokens
	waitTime := time.Duration(tokensNeeded * b.nanosPerToken)
	time.Sleep(waitTime)

	// Update state after sleep
	now := time.Now().UnixNano()
	atomic.StoreInt64(&b.lastLeakNano, now)
	atomic.StoreInt64(&b.tokens, b.limit-n)
}
