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
	"errors"
	"fmt"
	"io"

	"github.com/aerospike/backup-go/models"
	"golang.org/x/time/rate"
)

const (
	// TODO: add Metrics and measure these values, to make it more accurate.
	readerBufferSize = 10000
	writerBufferSize = 10000
)

// Reader describes data readers. To exit worker, the Reader must return io.EOF.
type Reader[T models.TokenConstraint] interface {
	Read() (T, error)
	Close()
}

// Writer describes data writers.
type Writer[T models.TokenConstraint] interface {
	Write(T) (n int, err error)
	Close() (err error)
}

// Processor describes data processors.
type Processor[T models.TokenConstraint] interface {
	Process(T) (T, error)
}

// The Chain contains a routine to process data. Chains will be running in parallel.
// Each routine is built from a Reader and/or Processor and/or Writer.
type Chain[T models.TokenConstraint] struct {
	routine func(context.Context) error
}

// Run execute the chain.
func (c *Chain[T]) Run(ctx context.Context) error {
	return c.routine(ctx)
}

// NewReaderBackupChain returns a new Chain with a Reader and a Processor,
// and communication channel for backup operation.
//
//nolint:gocritic // No need to give names to the result here.
func NewReaderBackupChain[T models.TokenConstraint](r Reader[T], p Processor[T]) (*Chain[T], chan T) {
	output := make(chan T, readerBufferSize)
	routine := newReaderBackupRoutine(r, p, output)

	return &Chain[T]{
		routine: routine,
	}, output
}

func newReaderBackupRoutine[T models.TokenConstraint](r Reader[T], p Processor[T], output chan<- T,
) func(context.Context) error {
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

					return fmt.Errorf("failed to read data: %w", err)
				}

				processed, err := p.Process(data)
				if err != nil {
					if errors.Is(err, models.ErrFilteredOut) {
						continue
					}

					return fmt.Errorf("failed to process data: %w", err)
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

// NewWriterBackupChain returns a new Chain with a Writer,
// and communication channel for backup operation.
//
//nolint:gocritic // No need to give names to the result here.
func NewWriterBackupChain[T models.TokenConstraint](w Writer[T], limiter *rate.Limiter) (*Chain[T], chan T) {
	input := make(chan T, writerBufferSize)
	routine := newWriterBackupRoutine(w, input, limiter)

	return &Chain[T]{
		routine: routine,
	}, input
}

func newWriterBackupRoutine[T models.TokenConstraint](w Writer[T], input <-chan T, limiter *rate.Limiter,
) func(context.Context) error {
	// Notice!
	// It is important to return func with `(err error)`,
	// otherwise the err variable will always be nil.
	// Don't replace it with `var err error`.
	return func(ctx context.Context) (err error) {
		defer func() {
			cErr := w.Close()
			// Process errors, not to lose any of them.
			switch {
			case err == nil:
				// In case the main error is nil, we assign a close error to it. (close error can be nil, it's ok)
				err = cErr
			case cErr != nil:
				// In case we have a Writer error and close error, we combine them into one error.
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
					return fmt.Errorf("failed to write data: %w", err)
				}

				if limiter != nil {
					if err := limiter.WaitN(ctx, n); err != nil {
						return fmt.Errorf("failed to limit data write: %w", err)
					}
				}
			}
		}
	}
}
