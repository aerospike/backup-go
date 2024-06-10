package pipeline

import (
	"context"

	"golang.org/x/time/rate"
)

// DataWriter is an interface for writing data to a destination.
//
//go:generate mockery --name DataWriter
type DataWriter[T any] interface {
	Write(T) (n int, err error)
	Close() (err error)
}

// writeWorker implements the pipeline.Worker interface
// It wraps a DataWriter and writes data to it
type writeWorker[T any] struct {
	DataWriter[T]
	receive <-chan T
	limiter *rate.Limiter
}

func NewWriteWorker[T any](writer DataWriter[T], limiter *rate.Limiter) Worker[T] {
	return &writeWorker[T]{
		DataWriter: writer,
		limiter:    limiter,
	}
}

// SetReceiveChan sets receive channel for the writeWorker
func (w *writeWorker[T]) SetReceiveChan(c <-chan T) {
	w.receive = c
}

// SetSendChan satisfies the pipeline.Worker interface
// but is a no-op for the writeWorker
func (w *writeWorker[T]) SetSendChan(_ chan<- T) {
	// no-op
}

// Run runs the writeWorker
func (w *writeWorker[T]) Run(ctx context.Context) error {
	defer w.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case data, active := <-w.receive:
			if !active {
				return nil
			}

			n, err := w.Write(data)
			if err != nil {
				return err
			}

			if w.limiter != nil {
				if err := w.limiter.WaitN(ctx, n); err != nil {
					return err
				}
			}
		}
	}
}
