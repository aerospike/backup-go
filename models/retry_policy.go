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
	"fmt"
	"math"
	"time"

	"golang.org/x/net/context"
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

// AttemptsLeft returns true if there are still retry attempts remaining.
// MaxRetries=0 means only the initial attempt (no retries).
// MaxRetries=N means initial attempt + N retries = N+1 total attempts.
func (p *RetryPolicy) AttemptsLeft(attempt uint) bool {
	if p == nil {
		return false
	}

	// If MaxRetries is 0, only attempt 0 (initial) is allowed
	if p.MaxRetries == 0 {
		return attempt == 0
	}

	return attempt < p.MaxRetries
}

// TotalAttempts returns the total number of attempts (initial + retries).
// Can be used to iterate over all attempts or logging.
func (p *RetryPolicy) TotalAttempts() uint {
	if p == nil || p.MaxRetries == 0 {
		return 1
	}
	return p.MaxRetries + 1
}

// Sleep waits for the calculated delay or until context is cancelled.
// Returns ctx.Err() if context was cancelled, nil if sleep completed.
func (p *RetryPolicy) Sleep(ctx context.Context, attempt uint) error {
	if p == nil {
		return nil
	}

	duration := p.calculateDelay(attempt)

	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// calculateDelay computes the delay for a given attempt number.
func (p *RetryPolicy) calculateDelay(attempt uint) time.Duration {
	delay := time.Duration(float64(p.BaseTimeout) * math.Pow(p.Multiplier, float64(attempt)))

	return delay
}
