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

type FanoutStrategy int

const (
	// Fixed default val
	Fixed FanoutStrategy = iota
	RoundRobin
	Split
)

// Fanout routes messages between chain pools.
// Depending on, FanoutStrategy messages can be distributed in different ways.
type Fanout[T models.TokenConstraint] struct {
	Inputs  []chan T
	Outputs []chan T

	strategy FanoutStrategy
	// for RoundRobin
	currentIndex uint64
}

// NewFanout returns a new Fanout.
// The Default strategy is RoundRobin.
func NewFanout[T models.TokenConstraint](
	inputs []chan T,
	outputs []chan T,
	strategy FanoutStrategy,
) (*Fanout[T], error) {
	f := &Fanout[T]{
		Inputs:   inputs,
		Outputs:  outputs,
		strategy: strategy, // Default
	}

	// Validations.
	if len(f.Outputs) == 0 {
		return nil, fmt.Errorf("no outputs provided")
	}

	if len(f.Inputs) == 0 {
		return nil, fmt.Errorf("no inputs provided")
	}

	if f.strategy == Fixed && len(f.Inputs) != len(f.Outputs) {
		return nil, fmt.Errorf("invalid inputs %d and outputs %d number for Fixed strategy",
			len(f.Inputs), len(f.Outputs))
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

// routeData routes a given piece of data based on the current fanout strategy (Fixed, RoundRobin, or Split).
func (f *Fanout[T]) routeData(ctx context.Context, index int, data T) {
	switch f.strategy {
	case Fixed: // Sen to current index.
	case RoundRobin:
		index = f.roundRobin(data)
	case Split:
		index = f.splitFunc(data)
	}

	select {
	case <-ctx.Done():
		return
	case f.Outputs[index] <- data: // ok.
	}
}

func (f *Fanout[T]) roundRobin(_ T) int {
	index := atomic.AddUint64(&f.currentIndex, 1) % uint64(len(f.Outputs))

	return int(index)
}

// splitFunc distributes token between pipeline workers for xdr backup.
func (f *Fanout[T]) splitFunc(token T) int {
	t, ok := any(token).(*models.ASBXToken)
	if !ok {
		return 0
	}

	partPerWorker := 4096 / len(f.Outputs)

	var id int
	if partPerWorker > 0 {
		id = t.Key.PartitionId() / partPerWorker
	}

	if id >= len(f.Outputs) {
		return id - 1
	}

	return id
}

// GetMetrics returns the accumulated length for input and output channels.
func (f *Fanout[T]) GetMetrics() (in, out int) {
	for _, input := range f.Inputs {
		in += len(input)
	}

	for _, output := range f.Outputs {
		out += len(output)
	}

	return in, out
}
