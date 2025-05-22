package pipe

import (
	"context"
	"sync"

	"github.com/aerospike/backup-go/models"
	"golang.org/x/time/rate"
)

// Pool is a pool of chains.
type Pool[T models.TokenConstraint] struct {
	Chains []*Chain[T]
	// Outputs and Inputs are mutually exclusive.
	Inputs  []chan T
	Outputs []chan T
}

func (p *Pool[T]) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errorCh := make(chan error)
	defer close(errorCh)

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

	for err := range errorCh {
		return err
	}

	return nil
}

type readerCreator[T models.TokenConstraint] func() reader[T]

type processorCreator[T models.TokenConstraint] func() processor[T]

type writerCreator[T models.TokenConstraint] func() writer[T]

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
