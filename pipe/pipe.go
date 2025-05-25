package pipe

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aerospike/backup-go/models"
	"golang.org/x/time/rate"
)

// Pipe is running and managing everything.
type Pipe[T models.TokenConstraint] struct {
	readPool  *Pool[T]
	writePool *Pool[T]
	fanout    *Fanout[T]
}

func NewBackupPipe[T models.TokenConstraint](
	pc ProcessorCreator[T],
	readers []Reader[T],
	writers []Writer[T],
	limiter *rate.Limiter,
) (*Pipe[T], error) {
	readPool := NewReaderBackupPool[T](readers, pc)
	writePool := NewWriterBackupPool[T](writers, limiter)
	// Swap channels!
	fanout, err := NewFanout[T](readPool.Outputs, writePool.Inputs, WithStrategy[T](RoundRobin))
	if err != nil {
		return nil, fmt.Errorf("failed to create fanout: %w", err)
	}

	return &Pipe[T]{
		readPool:  readPool,
		writePool: writePool,
		fanout:    fanout,
	}, nil
}

func (p *Pipe[T]) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	errorCh := make(chan error, 3)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(3)

	go func() {
		defer wg.Done()

		err := p.readPool.Run(ctx)
		if err != nil {
			errorCh <- fmt.Errorf("read pool failed: %w", err)

			cancel()

			return
		}
	}()

	go func() {
		defer wg.Done()

		err := p.writePool.Run(ctx)
		if err != nil {
			errorCh <- fmt.Errorf("write pool failed: %w", err)

			cancel()

			return
		}
	}()

	go func() {
		defer wg.Done()
		p.fanout.Run(ctx)
	}()

	wg.Wait()

	close(errorCh)

	var errs []error

	for err := range errorCh {
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
