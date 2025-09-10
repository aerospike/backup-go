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
	return NewRetryPolicy(1000*time.Millisecond, 1, 3)
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

	if p.MaxRetries < 1 {
		return fmt.Errorf("max retries must be greater than 0")
	}

	return nil
}

// Sleep waits for the specified number of retry attempts.
func (p *RetryPolicy) Sleep(attempt uint) {
	if p == nil {
		return
	}

	duration := time.Duration(float64(p.BaseTimeout) * math.Pow(p.Multiplier, float64(attempt)))
	time.Sleep(duration)
}

// AttemptsLeft returns true if there are still retry attempts left.
func (p *RetryPolicy) AttemptsLeft(attempt uint) bool {
	if p == nil {
		return false
	}

	// If MaxRetries is 0, then at least one retry attempt is made.
	if p.MaxRetries == 0 && attempt == 0 {
		return true
	}

	return attempt < p.MaxRetries
}
