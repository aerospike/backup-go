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

package models

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// RetryPolicy defines the configuration for retry attempts in case of failures.
type RetryPolicy struct {
	// BaseTimeout is the initial delay between retry attempts.
	BaseTimeout time.Duration

	// Multiplier is used to increase the delay between subsequent retry attempts.
	// The actual delay is calculated as: BaseTimeout * (Multiplier ^ attemptNumber)
	Multiplier float64

	// MaxRetries is the maximum number of retry attempts that will be made.
	// If set to 0, no retries will be performed.
	MaxRetries uint
}

// NewRetryPolicy returns new configuration for retry attempts in case of failures.
func NewRetryPolicy(baseTimeout time.Duration, multiplier float64, maxRetries uint) *RetryPolicy {
	return &RetryPolicy{
		BaseTimeout: baseTimeout,
		Multiplier:  multiplier,
		MaxRetries:  maxRetries,
	}
}

// NewDefaultRetryPolicy returns a new RetryPolicy with default values.
func NewDefaultRetryPolicy() *RetryPolicy {
	return NewRetryPolicy(1000*time.Millisecond, 2.0, 3)
}

// Validate checks retry policy values.
func (p *RetryPolicy) Validate() error {
	if p == nil {
		return nil
	}

	if p.BaseTimeout < 0 {
		return fmt.Errorf("base timeout must be non-negative")
	}

	if p.Multiplier < 1 {
		return fmt.Errorf("multiplier must be greater than 0")
	}

	// MaxRetries validation removed - 0 is valid (means no retries)

	return nil
}

// totalAttempts returns the total number of attempts (initial + retries).
// Can be used to iterate over all attempts or logging.
func (p *RetryPolicy) totalAttempts() uint {
	if p == nil || p.MaxRetries == 0 {
		return 1
	}

	return p.MaxRetries
}

// sleep waits for the calculated delay or until context is cancelled.
// Returns ctx.Err() if context was cancelled, nil if sleep completed.
func (p *RetryPolicy) sleep(ctx context.Context, attempt uint) error {
	if p == nil {
		return nil
	}

	duration := p.calculateDelay(attempt)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(duration):
		return nil
	}
}

// calculateDelay computes the delay with added jitter to prevent thundering herd.
// Jitter is +-10% of the calculated delay.
func (p *RetryPolicy) calculateDelay(attempt uint) time.Duration {
	baseDelay := time.Duration(float64(p.BaseTimeout) * math.Pow(p.Multiplier, float64(attempt)))

	jitter := p.calculateJitter(baseDelay)
	delay := max(baseDelay+jitter, 0)

	return delay
}

// calculateJitter computes the jitter to prevent thundering herd.
func (p *RetryPolicy) calculateJitter(baseDelay time.Duration) time.Duration {
	// Add +-10% jitter.
	jitterPercent := 0.1
	jitterAmount := time.Duration(float64(baseDelay) * jitterPercent)

	//nolint:gosec // rand is used for jitter, not critical for security.
	jitter := time.Duration(rand.Int64N(int64(jitterAmount*2))) - jitterAmount

	return jitter
}

// Do executes the operation with automatic retry logic.
// The operation is retried up to MaxRetries times with exponential backoff and jitter.
// Returns operation error if all retries are exhausted.
func (p *RetryPolicy) Do(ctx context.Context, operation func() error) error {
	if p == nil {
		return operation()
	}

	var lastErr error

	totalAttempts := p.totalAttempts()

	for attempt := range totalAttempts {
		// Execute operation.
		lastErr = operation()
		if lastErr == nil {
			// Success.
			return nil
		}

		// If this was the last attempt, exit.
		if attempt >= totalAttempts-1 {
			break
		}

		// sleep before the next attempt. We check context only in sleep, not to slow down first operation.
		if err := p.sleep(ctx, attempt); err != nil {
			// Show all errors if any.
			return errors.Join(err, lastErr)
		}
	}

	return fmt.Errorf("failed after %d attempt(s): %w", totalAttempts, lastErr)
}
