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
)

// Fanout routes messages between chain pools.
// FanoutStrategy controls the distribution of messages to output channels.
type Fanout struct {
	Inputs  []chan *models.Token
	Outputs []chan *models.Token

	strategy FanoutStrategy
	// for RoundRobin
	currentIndex atomic.Uint64
}

// NewFanout returns a new Fanout.
func NewFanout(
	inputs []chan *models.Token,
	outputs []chan *models.Token,
	strategy FanoutStrategy,
) (*Fanout, error) {
	f := &Fanout{
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

	if f.strategy != Fixed && f.strategy != RoundRobin {
		return nil, fmt.Errorf("unsupported fanout strategy: %d", f.strategy)
	}

	return f, nil
}

// Run starts routing messages in separate goroutines based on the defined fanout strategy.
func (f *Fanout) Run(ctx context.Context) {
	var wg sync.WaitGroup

	for i, input := range f.Inputs {
		wg.Go(func() {
			f.processInput(ctx, i, input)
		})
	}

	wg.Wait()
	f.Close()
}

// Close closes all output channels.
func (f *Fanout) Close() {
	for _, output := range f.Outputs {
		close(output)
	}
}

// processInput listens for incoming data on the input channel
// and routes it based on the fanout strategy or context state.
func (f *Fanout) processInput(ctx context.Context, index int, input <-chan *models.Token) {
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
func (f *Fanout) routeData(ctx context.Context, index int, data *models.Token) {
	switch f.strategy {
	case Fixed: // Send it to the current index.
	case RoundRobin:
		index = f.roundRobin(data)
	}

	select {
	case <-ctx.Done():
		return
	case f.Outputs[index] <- data: // ok.
	}
}

// roundRobin returns the next output chain index, distributing tokens in a fair, rotating manner.
func (f *Fanout) roundRobin(_ *models.Token) int {
	index := f.currentIndex.Add(1) % uint64(len(f.Outputs))

	return int(index)
}

// GetMetrics returns the accumulated length for input and output channels.
func (f *Fanout) GetMetrics() (in, out int) {
	if f.Inputs != nil {
		for _, input := range f.Inputs {
			in += len(input)
		}
	}

	if f.Outputs != nil {
		for _, output := range f.Outputs {
			out += len(output)
		}
	}

	return in, out
}
