package pipe

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aerospike/backup-go/models"
)

// Pipe is running and managing everything.
type Pipe[T models.TokenConstraint] struct {
	readPool  *Pool[T]
	writePool *Pool[T]
	fanout    *Fanout[T]
}

func NewBackupPipe[T models.TokenConstraint](
	parallelRead, parallelWrite uint,
	rc readerCreator[T],
	pc processorCreator[T],
	wc writerCreator[T],
) *Pipe[T] {
	readPool := NewReaderBackupPool[T](parallelRead, rc, pc)
	writePool := NewWriterBackupPool[T](parallelWrite, wc, nil)
	// Swap channels!
	fanout := NewFanout[T](readPool.Outputs, writePool.Inputs, WithStrategy[T](FanoutStrategyRoundRobin))

	return &Pipe[T]{
		readPool:  readPool,
		writePool: writePool,
		fanout:    fanout,
	}
}

func (p *Pipe[T]) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	errCh := make(chan error, 3)
	defer close(errCh)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(3)

	go func() {
		defer wg.Done()
		err := p.readPool.Run(ctx)
		if err != nil {
			errCh <- fmt.Errorf("read pool failed: %w", err)
			cancel()
			return
		}

		return
	}()

	go func() {
		defer wg.Done()
		err := p.writePool.Run(ctx)
		if err != nil {
			errCh <- fmt.Errorf("write pool failed: %w", err)
			cancel()
			return
		}

		return
	}()

	go func() {
		defer wg.Done()
		err := p.fanout.Run(ctx)
		if err != nil {
			errCh <- fmt.Errorf("fanout failed: %w", err)
			cancel()
			return
		}

		return
	}()

	wg.Wait()

	var errs []error
	for err := range errCh {
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
