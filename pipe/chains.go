package pipe

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aerospike/backup-go/models"
	"golang.org/x/time/rate"
)

const (
	readerBufferSize = 10000
	writerBufferSize = 10000
)

// ErrFilteredOut is returned by a processor when a token
// should be filtered out of the pipeline.
var ErrFilteredOut = errors.New("filtered out")

// Reader describes data readers. To exit worker, the reader must return io.EOF.
type reader[T models.TokenConstraint] interface {
	Read() (T, error)
	Close()
}

// writer describes data writers.
type writer[T models.TokenConstraint] interface {
	Write(T) (n int, err error)
	Close() (err error)
}

// describes data processors.
type processor[T models.TokenConstraint] interface {
	Process(T) (T, error)
}

type Chain[T models.TokenConstraint] struct {
	routine func(context.Context) error
}

func (c *Chain[T]) Run(ctx context.Context) error {
	return c.routine(ctx)
}

func NewReaderBackupChain[T models.TokenConstraint](r reader[T], p processor[T]) (*Chain[T], chan T) {
	output := make(chan T, readerBufferSize)
	routine := newReaderBackupRoutine(r, p, output)

	return &Chain[T]{
		routine: routine,
	}, output
}

func newReaderBackupRoutine[T models.TokenConstraint](r reader[T], p processor[T], output chan<- T) func(context.Context) error {
	return func(ctx context.Context) error {
		defer r.Close()
		defer close(output)

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				data, err := r.Read()
				if err != nil {
					if errors.Is(err, io.EOF) {

						return nil
					}

					return err
				}

				processed, err := p.Process(data)
				if err != nil {
					if errors.Is(err, ErrFilteredOut) {

						continue
					}
					return err
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case output <- processed:
				}
			}

		}
	}
}

func NewWriterBackupChain[T models.TokenConstraint](w writer[T], limiter *rate.Limiter) (*Chain[T], chan T) {
	input := make(chan T, writerBufferSize)
	routine := newWriterBackupRoutine(w, input, limiter)

	return &Chain[T]{
		routine: routine,
	}, input
}

func newWriterBackupRoutine[T models.TokenConstraint](w writer[T], input <-chan T, limiter *rate.Limiter) func(context.Context) error {
	return func(ctx context.Context) error {
		var err error

		defer func() {
			cErr := w.Close()
			// TODO: rewrite this switch, i don't like it!
			switch {
			case err == nil:
				err = cErr
			case cErr != nil:
				err = fmt.Errorf("write error: %w, close error: %w", err, cErr)
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case data, ok := <-input:
				if !ok {
					return nil
				}

				n, err := w.Write(data)
				if err != nil {
					return err
				}

				if limiter != nil {
					if err := limiter.WaitN(ctx, n); err != nil {
						return err
					}
				}
			}
		}
	}
}
