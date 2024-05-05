package pipeline

import (
	"context"
	"golang.org/x/time/rate"
	"time"
)

type RateLimiterInterface interface {
	Wait(ctx context.Context) error
}

type RateLimiter struct {
	*rate.Limiter
}

type NoOpLimiter struct{}

func (nl *NoOpLimiter) Wait(_ context.Context) error {
	return nil
}

func NewRateLimiter(requestsPerSecond int) RateLimiterInterface {
	if requestsPerSecond != 0 {
		rate.NewLimiter(rate.Every(time.Second/time.Duration(requestsPerSecond)), 1)
	}
	return &NoOpLimiter{}
}
