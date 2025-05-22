package pipe

import (
	"context"
	"sync"

	"github.com/aerospike/backup-go/models"
)

// RouteRule returns the output channel index for a given token.
type RouteRule[T models.TokenConstraint] func(T) int

type FanoutStrategy int

const (
	// FanoutStrategyStraight default val
	FanoutStrategyStraight FanoutStrategy = iota
	FanoutStrategyRoundRobin
	FanoutStrategyCustomRule
)

type Fanout[T models.TokenConstraint] struct {
	Inputs  []chan T
	Outputs []chan T

	strategy FanoutStrategy
	// for FanoutStrategyCustomRule
	rule RouteRule[T]
	// for FanoutStrategyRoundRobin
	currentIndex int
	indexMu      sync.Mutex
}

type FanoutOption[T models.TokenConstraint] func(*Fanout[T])

func WithRule[T models.TokenConstraint](rule RouteRule[T]) FanoutOption[T] {
	return func(f *Fanout[T]) {
		f.rule = rule
	}
}

func WithStrategy[T models.TokenConstraint](strategy FanoutStrategy) FanoutOption[T] {
	return func(f *Fanout[T]) {
		f.strategy = strategy
	}
}

func NewFanout[T models.TokenConstraint](
	inputs []chan T,
	outputs []chan T,
	options ...FanoutOption[T],
) *Fanout[T] {
	f := &Fanout[T]{
		Inputs:   inputs,
		Outputs:  outputs,
		strategy: FanoutStrategyRoundRobin, // Default
	}

	for _, option := range options {
		option(f)
	}

	return f
}

func (f *Fanout[T]) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	for i, input := range f.Inputs {
		wg.Add(1)

		index := i

		go func(in <-chan T) {
			defer wg.Done()
			f.processInput(ctx, index, in)
		}(input)
	}

	wg.Wait()
	f.Close()

	return nil
}

func (f *Fanout[T]) Close() {
	for _, output := range f.Outputs {
		close(output)
	}
}

func (f *Fanout[T]) processInput(ctx context.Context, index int, input <-chan T) {
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-input:
			if !ok {
				return
			}

			f.routeData(ctx, index, data)
		}
	}
}

func (f *Fanout[T]) routeData(ctx context.Context, index int, data T) {
	switch f.strategy {
	case FanoutStrategyStraight:
		f.routeStraightData(ctx, index, data)
	case FanoutStrategyRoundRobin:
		f.routeRoundRobinData(ctx, data)
	case FanoutStrategyCustomRule:
		f.routeCustomRuleData(ctx, data)
	}
}

func (f *Fanout[T]) routeStraightData(ctx context.Context, index int, data T) {
	if len(f.Outputs) == 0 {
		return
	}

	select {
	case <-ctx.Done():
		return
	case f.Outputs[index] <- data:
		// ok.
	}
}

func (f *Fanout[T]) routeRoundRobinData(ctx context.Context, data T) {
	if len(f.Outputs) == 0 {
		return
	}

	f.indexMu.Lock()
	index := f.currentIndex
	f.currentIndex = (f.currentIndex + 1) % len(f.Outputs)
	f.indexMu.Unlock()

	select {
	case <-ctx.Done():
		return
	case f.Outputs[index] <- data:
		// ok.
	}
}

func (f *Fanout[T]) routeCustomRuleData(ctx context.Context, data T) {
	if len(f.Outputs) == 0 || f.rule == nil {
		return
	}

	index := f.rule(data)

	select {
	case <-ctx.Done():
		return
	case f.Outputs[index] <- data:
		// ok.
	}
}
