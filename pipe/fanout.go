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

package pipe

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aerospike/backup-go/models"
)

// RouteRule returns the output channel index for a given token.
type RouteRule[T models.TokenConstraint] func(T) int

type FanoutStrategy int

const (
	// Straight default val
	Straight FanoutStrategy = iota
	RoundRobin
	CustomRule
)

// Fanout routes messages between chain pools.
// Depending on, FanoutStrategy messages can be distributed in different ways.
type Fanout[T models.TokenConstraint] struct {
	Inputs  []chan T
	Outputs []chan T

	strategy FanoutStrategy
	// for CustomRule
	rule RouteRule[T]
	// for RoundRobin
	currentIndex uint64
}

// FanoutOption describes options for Fanout.
type FanoutOption[T models.TokenConstraint] func(*Fanout[T])

// WithRule sets a CustomRule strategy and assigns custom rule to route messages.
func WithRule[T models.TokenConstraint](rule RouteRule[T]) FanoutOption[T] {
	return func(f *Fanout[T]) {
		f.rule = rule
		f.strategy = CustomRule
	}
}

// WithStrategy sets the distribution strategy for a Fanout instance.
func WithStrategy[T models.TokenConstraint](strategy FanoutStrategy) FanoutOption[T] {
	return func(f *Fanout[T]) {
		f.strategy = strategy
	}
}

// NewFanout returns a new Fanout.
// The Default strategy is RoundRobin.
func NewFanout[T models.TokenConstraint](
	inputs []chan T,
	outputs []chan T,
	options ...FanoutOption[T],
) (*Fanout[T], error) {
	f := &Fanout[T]{
		Inputs:   inputs,
		Outputs:  outputs,
		strategy: RoundRobin, // Default
	}

	for _, option := range options {
		option(f)
	}

	// Validations.
	if len(f.Outputs) == 0 {
		return nil, fmt.Errorf("no outputs provided")
	}

	if len(f.Inputs) == 0 {
		return nil, fmt.Errorf("no inputs provided")
	}

	switch f.strategy {
	case Straight:
		if len(f.Outputs) != len(f.Inputs) {
			return nil, fmt.Errorf("invalid inputs %d and outputs %d number for Straight strategy",
				len(f.Inputs), len(f.Outputs))
		}
	case CustomRule:
		if f.rule == nil {
			return nil, fmt.Errorf("custom rule is required for CustomRule strategy")
		}
	default: // ok.
	}

	return f, nil
}

// Run starts routing messages in separate goroutines based on the defined fanout strategy.
func (f *Fanout[T]) Run(ctx context.Context) {
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
}

// Close closes all output channels.
func (f *Fanout[T]) Close() {
	for _, output := range f.Outputs {
		close(output)
	}
}

// processInput listens for incoming data on the input channel
// and routes it based on the fanout strategy or context state.
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

// routeData routes a given piece of data based on the current fanout strategy (Straight, RoundRobin, or CustomRule).
func (f *Fanout[T]) routeData(ctx context.Context, index int, data T) {
	switch f.strategy {
	case Straight:
		f.routeStraightData(ctx, index, data)
	case RoundRobin:
		f.routeRoundRobinData(ctx, data)
	case CustomRule:
		f.routeCustomRuleData(ctx, data)
	}
}

// routeStraightData routes a given piece of data to the output channel at the given index.
func (f *Fanout[T]) routeStraightData(ctx context.Context, index int, data T) {
	select {
	case <-ctx.Done():
		return
	case f.Outputs[index] <- data: // ok.
	}
}

// routeRoundRobinData routes a given piece of data to the output channel at the next index in round-robin fashion.
func (f *Fanout[T]) routeRoundRobinData(ctx context.Context, data T) {
	index := atomic.AddUint64(&f.currentIndex, 1) % uint64(len(f.Outputs))

	select {
	case <-ctx.Done():
		return
	case f.Outputs[index] <- data: // ok.
	}
}

// routeCustomRuleData routes a given piece of data to the output channel based on the custom rule.
func (f *Fanout[T]) routeCustomRuleData(ctx context.Context, data T) {
	index := f.rule(data)

	select {
	case <-ctx.Done():
		return
	case f.Outputs[index] <- data: // ok.
	}
}

func (f *Fanout[T]) GetMetrics() (in, out int) {
	for _, input := range f.Inputs {
		in += len(input)
	}

	for _, output := range f.Outputs {
		out += len(output)
	}

	return in, out
}
