package processors

import (
	"context"

	"golang.org/x/time/rate"
)

// tpsLimiter is a type representing a Token Per Second limiter.
// it does not allow processing more than tps amount of tokens per second.
type tpsLimiter[T any] struct {
	ctx     context.Context
	limiter *rate.Limiter
	tps     int
}

// NewTPSLimiter Create a new TPS limiter.
// n — allowed  number of tokens per second, n = 0 means no limit.
func NewTPSLimiter[T any](ctx context.Context, n int) DataProcessor[T] {
	if n == 0 {
		return &noopProcessor[T]{}
	}

	return &tpsLimiter[T]{
		ctx:     ctx,
		tps:     n,
		limiter: rate.NewLimiter(rate.Limit(n), 1),
	}
}

// Process delays pipeline if it's needed to match desired rate.
func (t *tpsLimiter[T]) Process(token T) (T, error) {
	if t.tps == 0 {
		return token, nil
	}

	if err := t.limiter.Wait(t.ctx); err != nil {
		var zero T
		return zero, err
	}

	return token, nil
}
