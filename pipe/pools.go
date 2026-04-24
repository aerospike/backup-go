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

	"github.com/aerospike/backup-go/internal/bandwidth"
	"github.com/aerospike/backup-go/models"
	"golang.org/x/sync/errgroup"
)

// Pool is a pool of chains.
// All chains in a pool are running in parallel.
// Pools are communicating via fanout.
type Pool[T models.TokenConstraint] struct {
	mu sync.RWMutex
	eg *errgroup.Group

	Chains []*Chain[T]
	// Outputs and Inputs are mutually exclusive.
	Inputs  []chan T
	Outputs []chan T
}

// Run runs all chains in the pool.
func (p *Pool[T]) Run(ctx context.Context) error {
	p.eg, ctx = errgroup.WithContext(ctx)

	p.mu.RLock()

	for i := range p.Chains {
		chain := p.Chains[i]

		p.eg.Go(func() error {
			return chain.Run(ctx)
		})
	}

	p.mu.RUnlock()

	return p.eg.Wait()
}

// ProcessorCreator is a function type that defines a creator for a Processor.
type ProcessorCreator[T models.TokenConstraint] func() Processor[T]

// NewReaderPool returns a new pool of Reader and Processor chains for backup operations,
// with the specified parallelism.
func NewReaderPool[T models.TokenConstraint](readers []Reader[T], pc ProcessorCreator[T]) *Pool[T] {
	chains := make([]*Chain[T], len(readers))
	outputs := make([]chan T, len(readers))

	for i := range readers {
		chains[i], outputs[i] = NewReaderChain[T](readers[i], pc())
	}

	return &Pool[T]{
		Chains:  chains,
		Outputs: outputs,
	}
}

// NewWriterPool creates a new pool of Writer chains for backup operations,
// with the specified parallelism and bandwidth.
func NewWriterPool[T models.TokenConstraint](writers []Writer[T], limiter *bandwidth.Limiter) *Pool[T] {
	chains := make([]*Chain[T], len(writers))
	inputs := make([]chan T, len(writers))

	for i := range writers {
		chains[i], inputs[i] = NewWriterChain[T](writers[i], limiter)
	}

	return &Pool[T]{
		Chains: chains,
		Inputs: inputs,
	}
}

// AddReader adds a new reader chain to the pool and starts it.
func (p *Pool[T]) AddReader(ctx context.Context, reader Reader[T], pc ProcessorCreator[T]) (chan T, error) {
	chain, output := NewReaderChain[T](reader, pc())

	p.mu.Lock()
	if p.eg == nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("pool is not running")
	}

	p.Chains = append(p.Chains, chain)
	p.Outputs = append(p.Outputs, output)
	p.mu.Unlock()

	p.eg.Go(func() error {
		return chain.Run(ctx)
	})

	return output, nil
}

// AddWriter adds a new writer chain to the pool and starts it.
func (p *Pool[T]) AddWriter(ctx context.Context, writer Writer[T], limiter *bandwidth.Limiter) (chan T, error) {
	chain, input := NewWriterChain[T](writer, limiter)

	p.mu.Lock()
	if p.eg == nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("pool is not running")
	}

	p.Chains = append(p.Chains, chain)
	p.Inputs = append(p.Inputs, input)
	p.mu.Unlock()

	p.eg.Go(func() error {
		return chain.Run(ctx)
	})

	return input, nil
}

// Close closing channels and cleaning links.
func (p *Pool[T]) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Nullify objects, so GC can free this memory.
	p.Chains = nil
	p.Inputs = nil
	p.Outputs = nil
}
