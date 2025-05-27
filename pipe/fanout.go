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

// FanoutStrategy represents a pipeline routing strategy.
type FanoutStrategy int

const (
	// Fixed strategy routes incoming tokens to output channels, establishing a
	// dedicated one-to-one mapping between input channels and output channels.
	// All tokens read from a specific input channel are routed to its pre-assigned
	// output channel. The number of output channels must equal the number of input
	// channels being processed.
	Fixed FanoutStrategy = iota
	// RoundRobin distributes incoming tokens between available output channels in a
	// fair, rotating manner.
	RoundRobin
	// Split routes incoming tokens to output channels using a custom routing function.
	// The routing function determines the destination channel for each token based on
	// its partition id.
	Split
)

// Fanout routes messages between chain pools.
// FanoutStrategy controls the distribution of messages to output channels.
type Fanout[T models.TokenConstraint] struct {
	Inputs  []chan T
	Outputs []chan T

	strategy FanoutStrategy
	// for RoundRobin
	currentIndex uint64
}

// NewFanout returns a new Fanout.
func NewFanout[T models.TokenConstraint](
	inputs []chan T,
	outputs []chan T,
	strategy FanoutStrategy,
) (*Fanout[T], error) {
	f := &Fanout[T]{
		Inputs:   inputs,
		Outputs:  outputs,
		strategy: strategy,
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

	if f.strategy != Fixed && f.strategy != RoundRobin && f.strategy != Split {
		return nil, fmt.Errorf("unsupported fanout strategy: %d", f.strategy)
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
	case Fixed: // Send it to the current index.
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

// roundRobin returns the next output chain index, distributing tokens in a fair, rotating manner.
func (f *Fanout[T]) roundRobin(_ T) int {
	index := atomic.AddUint64(&f.currentIndex, 1) % uint64(len(f.Outputs))

	return int(index)
}

// splitFunc determines an output chain for the token based on its partition id.
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
