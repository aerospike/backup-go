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
		return fmt.Errorf("base timeout must be positive")
	}

	if p.Multiplier < 1 {
		return fmt.Errorf("multiplier must be positive")
	}

	if p.MaxRetries < 1 {
		return fmt.Errorf("max retries must be greater than or equal to 1")
	}

	return nil
}
