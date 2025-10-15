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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testBaseTimeout = 100 * time.Millisecond
	testMultiplier  = 2.0
	testMaxRetries  = 3
)

func TestNewRetryPolicy(t *testing.T) {
	t.Parallel()

	t.Run("Creates policy with given values", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, testMultiplier, testMaxRetries)

		require.NotNil(t, policy)
		require.Equal(t, testBaseTimeout, policy.BaseTimeout)
		require.Equal(t, testMultiplier, policy.Multiplier)
		require.Equal(t, uint(testMaxRetries), policy.MaxRetries)
	})

	t.Run("Creates policy with zero values", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(0, 1.0, 0)

		require.NotNil(t, policy)
		require.Equal(t, time.Duration(0), policy.BaseTimeout)
		require.Equal(t, 1.0, policy.Multiplier)
		require.Equal(t, uint(0), policy.MaxRetries)
	})

	t.Run("Creates policy with large values", func(t *testing.T) {
		t.Parallel()

		largeTimeout := 1 * time.Hour
		largeMultiplier := 10.0
		largeMaxRetries := uint(1000)

		policy := NewRetryPolicy(largeTimeout, largeMultiplier, largeMaxRetries)

		require.NotNil(t, policy)
		require.Equal(t, largeTimeout, policy.BaseTimeout)
		require.Equal(t, largeMultiplier, policy.Multiplier)
		require.Equal(t, largeMaxRetries, policy.MaxRetries)
	})

	t.Run("Creates policy with fractional multiplier", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, 1.5, testMaxRetries)

		require.NotNil(t, policy)
		require.Equal(t, 1.5, policy.Multiplier)
	})
}

func TestNewDefaultRetryPolicy(t *testing.T) {
	t.Parallel()

	t.Run("Creates policy with default values", func(t *testing.T) {
		t.Parallel()

		policy := NewDefaultRetryPolicy()

		require.NotNil(t, policy)
		require.Equal(t, 1000*time.Millisecond, policy.BaseTimeout)
		require.Equal(t, 2.0, policy.Multiplier)
		require.Equal(t, uint(3), policy.MaxRetries)
	})
}

func TestRetryPolicy_Validate(t *testing.T) {
	t.Parallel()

	t.Run("Valid policy passes validation", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, testMultiplier, testMaxRetries)

		err := policy.Validate()

		require.NoError(t, err)
	})

	t.Run("Nil policy passes validation", func(t *testing.T) {
		t.Parallel()

		var policy *RetryPolicy

		err := policy.Validate()

		require.NoError(t, err)
	})

	t.Run("Zero max retries passes validation", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, testMultiplier, 0)

		err := policy.Validate()

		require.NoError(t, err)
	})

	t.Run("Negative base timeout fails validation", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(-1*time.Second, testMultiplier, testMaxRetries)

		err := policy.Validate()

		require.Error(t, err)
		require.Contains(t, err.Error(), "base timeout must be non-negative")
	})

	t.Run("Zero multiplier fails validation", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, 0, testMaxRetries)

		err := policy.Validate()

		require.Error(t, err)
		require.Contains(t, err.Error(), "multiplier must be greater than 0")
	})

	t.Run("Negative multiplier fails validation", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, -1.5, testMaxRetries)

		err := policy.Validate()

		require.Error(t, err)
		require.Contains(t, err.Error(), "multiplier must be greater than 0")
	})

	t.Run("Multiplier less than 1 fails validation", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, 0.5, testMaxRetries)

		err := policy.Validate()

		require.Error(t, err)
		require.Contains(t, err.Error(), "multiplier must be greater than 0")
	})

	t.Run("Valid policy with minimum values", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(0, 1.0, 0)

		err := policy.Validate()

		require.NoError(t, err)
	})

	t.Run("Valid policy with large max retries", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, testMultiplier, 10000)

		err := policy.Validate()

		require.NoError(t, err)
	})
}

func TestRetryPolicy_Do(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		policy         *RetryPolicy
		operation      func() (callCount *int, operation func() error)
		contextTimeout time.Duration
		wantErr        bool
		wantCallCount  int
	}{
		{
			name:   "success on first attempt",
			policy: NewRetryPolicy(10*time.Millisecond, 2.0, 3),
			operation: func() (callCount *int, operation func() error) {
				count := 0
				return &count, func() error {
					count++
					return nil
				}
			},
			wantErr:       false,
			wantCallCount: 1,
		},
		{
			name:   "success on second attempt",
			policy: NewRetryPolicy(10*time.Millisecond, 2.0, 3),
			operation: func() (callCount *int, operation func() error) {
				count := 0
				return &count, func() error {
					count++
					if count < 2 {
						return errors.New("temporary error")
					}
					return nil
				}
			},
			wantErr:       false,
			wantCallCount: 2,
		},
		{
			name:   "success on third attempt",
			policy: NewRetryPolicy(10*time.Millisecond, 2.0, 3),
			operation: func() (callCount *int, operation func() error) {
				count := 0
				return &count, func() error {
					count++
					if count < 3 {
						return errors.New("temporary error")
					}
					return nil
				}
			},
			wantErr:       false,
			wantCallCount: 3,
		},
		{
			name:   "all attempts fail",
			policy: NewRetryPolicy(10*time.Millisecond, 2.0, 2),
			operation: func() (callCount *int, operation func() error) {
				count := 0
				return &count, func() error {
					count++
					return errors.New("persistent error")
				}
			},
			wantErr:       true,
			wantCallCount: 2,
		},
		{
			name:   "no retries (MaxRetries=0)",
			policy: NewRetryPolicy(10*time.Millisecond, 2.0, 0),
			operation: func() (callCount *int, operation func() error) {
				count := 0
				return &count, func() error {
					count++
					return errors.New("error on first attempt")
				}
			},
			wantErr:       true,
			wantCallCount: 1,
		},
		{
			name:   "context cancelled during retry",
			policy: NewRetryPolicy(100*time.Millisecond, 2.0, 5),
			operation: func() (callCount *int, operation func() error) {
				count := 0
				return &count, func() error {
					count++
					return errors.New("error that triggers retry")
				}
			},
			// Cancel during sleep
			contextTimeout: 150 * time.Millisecond,
			wantErr:        true,
			wantCallCount:  2,
		},
		{
			name:   "nil policy executes once",
			policy: nil,
			operation: func() (callCount *int, operation func() error) {
				count := 0
				return &count, func() error {
					count++
					return nil
				}
			},
			wantErr:       false,
			wantCallCount: 1,
		},
		{
			name:   "nil policy with error executes once",
			policy: nil,
			operation: func() (callCount *int, operation func() error) {
				count := 0
				return &count, func() error {
					count++
					return errors.New("error")
				}
			},
			wantErr:       true,
			wantCallCount: 1,
		},
		{
			name:   "success with MaxRetries=1",
			policy: NewRetryPolicy(10*time.Millisecond, 2.0, 1),
			operation: func() (callCount *int, operation func() error) {
				count := 0
				return &count, func() error {
					count++
					return nil
				}
			},
			wantErr:       false,
			wantCallCount: 1,
		},
		{
			name:   "all attempts fail with MaxRetries=1",
			policy: NewRetryPolicy(10*time.Millisecond, 2.0, 1),
			operation: func() (callCount *int, operation func() error) {
				count := 0
				return &count, func() error {
					count++
					return errors.New("persistent error")
				}
			},
			wantErr:       true,
			wantCallCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			if tt.contextTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.contextTimeout)
				defer cancel()
			}

			callCount, operation := tt.operation()

			err := tt.policy.Do(ctx, operation)

			if (err != nil) != tt.wantErr {
				t.Errorf("Do() error = %v, wantErr %v", err, tt.wantErr)
			}

			if *callCount != tt.wantCallCount {
				t.Errorf("Do() callCount = %v, want %v", *callCount, tt.wantCallCount)
			}
		})
	}
}

func TestRetryPolicy_Do_ErrorMessages(t *testing.T) {
	t.Parallel()

	t.Run("error message contains attempt count", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(10*time.Millisecond, 2.0, 2)
		ctx := context.Background()

		err := policy.Do(ctx, func() error {
			return errors.New("test error")
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "failed after 2 attempt(s)")
		require.Contains(t, err.Error(), "test error")
	})

	t.Run("context error is joined with last operation error", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(100*time.Millisecond, 2.0, 5)
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()

		err := policy.Do(ctx, func() error {
			return errors.New("operation error")
		})

		require.Error(t, err)
		// Should contain both context error and operation error
		require.Contains(t, err.Error(), "context deadline exceeded")
		require.Contains(t, err.Error(), "operation error")
	})
}

func TestRetryPolicy_Do_ThreadSafety(t *testing.T) {
	t.Parallel()

	policy := NewRetryPolicy(10*time.Millisecond, 2.0, 3)
	const numGoroutines = 100
	errChan := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			ctx := context.Background()
			callCount := 0

			err := policy.Do(ctx, func() error {
				callCount++
				if callCount < 2 {
					return errors.New("temporary error")
				}
				return nil
			})

			errChan <- err
		}()
	}

	// Collect results
	for i := 0; i < numGoroutines; i++ {
		err := <-errChan
		require.NoError(t, err)
	}
}
