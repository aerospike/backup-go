package pipe

import (
	"context"
	"fmt"
	"sync"

	"github.com/aerospike/backup-go/models"
	"golang.org/x/time/rate"
)

// Pool is a pool of chains.
// All chains in a pool are running in parallel.
// Pools are communicating via fanout.
type Pool[T models.TokenConstraint] struct {
	Chains []*Chain[T]
	// Outputs and Inputs are mutually exclusive.
	Inputs  []chan T
	Outputs []chan T
}

// Run runs all chains in the pool.
func (p *Pool[T]) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errorCh := make(chan error, len(p.Chains))

	var wg sync.WaitGroup
	for i := range p.Chains {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := p.Chains[i].Run(ctx); err != nil {
				errorCh <- err
				// Shut down all chains on one error.
				cancel()
			}
		}()
	}

	wg.Wait()

	close(errorCh)

	for err := range errorCh {
		return fmt.Errorf("failed to run chains: %w", err)
	}

	return nil
}

// ProcessorCreator is a function type that defines a creator for a Processor.
type ProcessorCreator[T models.TokenConstraint] func() Processor[T]

// NewReaderBackupPool returns a new pool of Reader and Processor chains for backup operations,
// with the specified parallelism.
func NewReaderBackupPool[T models.TokenConstraint](readers []Reader[T], pc ProcessorCreator[T]) *Pool[T] {
	chains := make([]*Chain[T], len(readers))
	outputs := make([]chan T, len(readers))

	for i := range readers {
		chains[i], outputs[i] = NewReaderBackupChain[T](readers[i], pc())
	}

	return &Pool[T]{
		Chains:  chains,
		Outputs: outputs,
	}
}

// NewWriterBackupPool creates a new pool of Writer chains for backup operations,
// with the specified parallelism and limiter.
func NewWriterBackupPool[T models.TokenConstraint](writers []Writer[T], limiter *rate.Limiter) *Pool[T] {
	chains := make([]*Chain[T], len(writers))
	inputs := make([]chan T, len(writers))

	for i := range writers {
		chains[i], inputs[i] = NewWriterBackupChain[T](writers[i], limiter)
	}

	return &Pool[T]{
		Chains: chains,
		Inputs: inputs,
	}
}
