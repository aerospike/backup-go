package models

import "time"

// RetryPolicy defines the configuration for retry attempts in case of failures.
type RetryPolicy struct {
	// BaseTimeout is the initial delay between retry attempts.
	BaseTimeout time.Duration

	// Multiplier is used to increase the delay between subsequent retry attempts.
	// The actual delay is calculated as: BaseTimeout * (Multiplier ^ attemptNumber)
	Multiplier float64

	// MaxRetries is the maximum number of retry attempts that will be made.
	// If set to 0, no retries will be performed.
	MaxRetries int
}
