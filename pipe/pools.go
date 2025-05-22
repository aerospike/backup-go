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

// readerCreator is a function that creates a reader.
type readerCreator[T models.TokenConstraint] func() reader[T]

// processorCreator is a function type that defines a creator for a processor.
type processorCreator[T models.TokenConstraint] func() processor[T]

// writerCreator is a function type that creates a writer.
type writerCreator[T models.TokenConstraint] func() writer[T]

// NewReaderBackupPool returns a new pool of reader and processor chains for backup operations,
// with the specified parallelism.
func NewReaderBackupPool[T models.TokenConstraint](parallel uint, rc readerCreator[T], pc processorCreator[T]) *Pool[T] {
	chains := make([]*Chain[T], parallel)
	outputs := make([]chan T, parallel)
	for i := range parallel {
		chains[i], outputs[i] = NewReaderBackupChain[T](rc(), pc())
	}

	return &Pool[T]{
		Chains:  chains,
		Outputs: outputs,
	}
}

// NewWriterBackupPool creates a new pool of writer chains for backup operations,
// with the specified parallelism and limiter.
func NewWriterBackupPool[T models.TokenConstraint](parallel uint, wc writerCreator[T], limiter *rate.Limiter) *Pool[T] {
	chains := make([]*Chain[T], parallel)
	inputs := make([]chan T, parallel)
	for i := range parallel {
		chains[i], inputs[i] = NewWriterBackupChain[T](wc(), limiter)
	}

	return &Pool[T]{
		Chains: chains,
		Inputs: inputs,
	}
}
