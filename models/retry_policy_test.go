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

func TestRetryPolicy_AttemptsLeft(t *testing.T) {
	t.Parallel()

	t.Run("Returns true for first attempt", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, testMultiplier, testMaxRetries)

		result := policy.AttemptsLeft(0)

		require.True(t, result)
	})

	t.Run("Returns true when attempts under max", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, testMultiplier, testMaxRetries)

		result := policy.AttemptsLeft(2)

		require.True(t, result)
	})

	t.Run("Returns false when attempts equal max", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, testMultiplier, testMaxRetries)

		result := policy.AttemptsLeft(3)

		require.False(t, result)
	})

	t.Run("Returns false when attempts exceed max", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, testMultiplier, testMaxRetries)

		result := policy.AttemptsLeft(10)

		require.False(t, result)
	})

	t.Run("Returns false for nil policy", func(t *testing.T) {
		t.Parallel()

		var policy *RetryPolicy

		result := policy.AttemptsLeft(0)

		require.False(t, result)
	})

	t.Run("Returns true for zero max retries on first attempt", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, testMultiplier, 0)

		result := policy.AttemptsLeft(0)

		require.True(t, result)
	})

	t.Run("Returns false for zero max retries on second attempt", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, testMultiplier, 0)

		result := policy.AttemptsLeft(1)

		require.False(t, result)
	})

	t.Run("Returns correct value for large max retries", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, testMultiplier, 1000)

		require.True(t, policy.AttemptsLeft(0))
		require.True(t, policy.AttemptsLeft(500))
		require.True(t, policy.AttemptsLeft(999))
		require.False(t, policy.AttemptsLeft(1000))
		require.False(t, policy.AttemptsLeft(1001))
	})

	t.Run("Returns correct value for max retries of 1", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, testMultiplier, 1)

		require.True(t, policy.AttemptsLeft(0))
		require.False(t, policy.AttemptsLeft(1))
	})
}

func TestRetryPolicy_TotalAttempts(t *testing.T) {
	t.Parallel()

	t.Run("Returns 1 for zero max retries", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, testMultiplier, 0)

		total := policy.TotalAttempts()

		require.Equal(t, uint(1), total)
	})

	t.Run("Returns correct total for non-zero max retries", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, testMultiplier, testMaxRetries)

		total := policy.TotalAttempts()

		require.Equal(t, uint(4), total) // 1 initial + 3 retries
	})

	t.Run("Returns 1 for nil policy", func(t *testing.T) {
		t.Parallel()

		var policy *RetryPolicy

		total := policy.TotalAttempts()

		require.Equal(t, uint(1), total)
	})

	t.Run("Returns correct total for max retries of 1", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, testMultiplier, 1)

		total := policy.TotalAttempts()

		require.Equal(t, uint(2), total) // 1 initial + 1 retry
	})

	t.Run("Returns correct total for large max retries", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(testBaseTimeout, testMultiplier, 999)

		total := policy.TotalAttempts()

		require.Equal(t, uint(1000), total)
	})
}

func TestRetryPolicy_Sleep(t *testing.T) {
	t.Parallel()

	t.Run("Sleeps for base timeout on first attempt", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(50*time.Millisecond, 1.0, testMaxRetries)
		ctx := context.Background()
		start := time.Now()

		err := policy.Sleep(ctx, 0)

		elapsed := time.Since(start)
		require.NoError(t, err)
		require.GreaterOrEqual(t, elapsed, 50*time.Millisecond)
		require.Less(t, elapsed, 100*time.Millisecond)
	})

	t.Run("Sleeps with exponential backoff", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(50*time.Millisecond, 2.0, testMaxRetries)
		ctx := context.Background()
		start := time.Now()

		err := policy.Sleep(ctx, 2) // 50ms * 2^2 = 200ms

		elapsed := time.Since(start)
		require.NoError(t, err)
		require.GreaterOrEqual(t, elapsed, 200*time.Millisecond)
		require.Less(t, elapsed, 250*time.Millisecond)
	})

	t.Run("Returns nil for nil policy", func(t *testing.T) {
		t.Parallel()

		var policy *RetryPolicy
		ctx := context.Background()
		start := time.Now()

		err := policy.Sleep(ctx, 0)

		elapsed := time.Since(start)
		require.NoError(t, err)
		require.Less(t, elapsed, 10*time.Millisecond)
	})

	t.Run("Returns early on context cancellation", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(1*time.Second, 1.0, testMaxRetries)
		ctx, cancel := context.WithCancel(context.Background())
		start := time.Now()

		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		err := policy.Sleep(ctx, 0)

		elapsed := time.Since(start)
		require.Error(t, err)
		require.Equal(t, context.Canceled, err)
		require.Less(t, elapsed, 200*time.Millisecond)
	})

	t.Run("Returns early on context timeout", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(1*time.Second, 1.0, testMaxRetries)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		start := time.Now()

		err := policy.Sleep(ctx, 0)

		elapsed := time.Since(start)
		require.Error(t, err)
		require.Equal(t, context.DeadlineExceeded, err)
		require.Less(t, elapsed, 200*time.Millisecond)
	})

	t.Run("Works with already cancelled context", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(1*time.Second, 1.0, testMaxRetries)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		start := time.Now()

		err := policy.Sleep(ctx, 0)

		elapsed := time.Since(start)
		require.Error(t, err)
		require.Equal(t, context.Canceled, err)
		require.Less(t, elapsed, 50*time.Millisecond)
	})

	t.Run("Sleeps with multiplier 1.0", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(50*time.Millisecond, 1.0, testMaxRetries)
		ctx := context.Background()
		start := time.Now()

		err := policy.Sleep(ctx, 5) // Should still be 50ms

		elapsed := time.Since(start)
		require.NoError(t, err)
		require.GreaterOrEqual(t, elapsed, 50*time.Millisecond)
		require.Less(t, elapsed, 100*time.Millisecond)
	})

	t.Run("Sleeps with large multiplier", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(10*time.Millisecond, 3.0, testMaxRetries)
		ctx := context.Background()
		start := time.Now()

		err := policy.Sleep(ctx, 3) // 10ms * 3^3 = 270ms

		elapsed := time.Since(start)
		require.NoError(t, err)
		require.GreaterOrEqual(t, elapsed, 270*time.Millisecond)
		require.Less(t, elapsed, 320*time.Millisecond)
	})

	t.Run("Sleeps with fractional multiplier", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(100*time.Millisecond, 1.5, testMaxRetries)
		ctx := context.Background()
		start := time.Now()

		err := policy.Sleep(ctx, 2) // 100ms * 1.5^2 = 225ms

		elapsed := time.Since(start)
		require.NoError(t, err)
		require.GreaterOrEqual(t, elapsed, 225*time.Millisecond)
		require.Less(t, elapsed, 275*time.Millisecond)
	})

	t.Run("Sleeps with zero base timeout", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(0, testMultiplier, testMaxRetries)
		ctx := context.Background()
		start := time.Now()

		err := policy.Sleep(ctx, 0)

		elapsed := time.Since(start)
		require.NoError(t, err)
		require.Less(t, elapsed, 10*time.Millisecond)
	})
}

func TestRetryPolicy_calculateDelay(t *testing.T) {
	t.Parallel()

	t.Run("Calculates correct delay for first attempt", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(100*time.Millisecond, 2.0, testMaxRetries)

		delay := policy.calculateDelay(0)

		require.Equal(t, 100*time.Millisecond, delay)
	})

	t.Run("Calculates correct delay with exponential backoff", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(100*time.Millisecond, 2.0, testMaxRetries)

		delay1 := policy.calculateDelay(1) // 100 * 2^1 = 200ms
		delay2 := policy.calculateDelay(2) // 100 * 2^2 = 400ms
		delay3 := policy.calculateDelay(3) // 100 * 2^3 = 800ms

		require.Equal(t, 200*time.Millisecond, delay1)
		require.Equal(t, 400*time.Millisecond, delay2)
		require.Equal(t, 800*time.Millisecond, delay3)
	})

	t.Run("Calculates correct delay with multiplier 1.0", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(100*time.Millisecond, 1.0, testMaxRetries)

		delay0 := policy.calculateDelay(0)
		delay5 := policy.calculateDelay(5)

		require.Equal(t, 100*time.Millisecond, delay0)
		require.Equal(t, 100*time.Millisecond, delay5)
	})

	t.Run("Calculates correct delay with fractional multiplier", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(100*time.Millisecond, 1.5, testMaxRetries)

		delay2 := policy.calculateDelay(2) // 100 * 1.5^2 = 225ms

		require.Equal(t, 225*time.Millisecond, delay2)
	})

	t.Run("Calculates correct delay with zero base timeout", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(0, 2.0, testMaxRetries)

		delay := policy.calculateDelay(5)

		require.Equal(t, time.Duration(0), delay)
	})

	t.Run("Calculates correct delay with large attempt number", func(t *testing.T) {
		t.Parallel()

		policy := NewRetryPolicy(10*time.Millisecond, 2.0, testMaxRetries)

		delay := policy.calculateDelay(10) // 10ms * 2^10 = 10240ms

		require.Equal(t, 10240*time.Millisecond, delay)
	})
}
