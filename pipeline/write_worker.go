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

package pipeline

import (
	"context"
	"fmt"

	"golang.org/x/time/rate"
)

// DataWriter is an interface for writing data to a destination.
//
//go:generate mockery --name DataWriter
type DataWriter[T any] interface {
	Write(T) (n int, err error)
	Close() (err error)
}

// writeWorker implements the pipeline.Worker interface.
// It wraps a DataWriter and writes data to it.
type writeWorker[T any] struct {
	DataWriter[T]
	receive <-chan T
	limiter *rate.Limiter
}

// NewWriteWorker creates a new Worker.
func NewWriteWorker[T any](writer DataWriter[T], limiter *rate.Limiter) Worker[T] {
	return &writeWorker[T]{
		DataWriter: writer,
		limiter:    limiter,
	}
}

// SetReceiveChan sets receive channel for the writeWorker.
func (w *writeWorker[T]) SetReceiveChan(c <-chan T) {
	w.receive = c
}

// SetSendChan satisfies the pipeline.Worker interface
// but is a no-op for the writeWorker.
func (w *writeWorker[T]) SetSendChan(_ chan<- T) {
	// no-op
}

// Run runs the writeWorker.
func (w *writeWorker[T]) Run(ctx context.Context) (err error) {
	defer func() {
		closeErr := w.Close()

		if err == nil {
			err = closeErr
		} else if closeErr != nil {
			err = fmt.Errorf("write error: %w, close error: %w", err, closeErr)
		}
	}()

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

// GetMetrics returns stats of received and sent messages.
func (w *writeWorker[T]) GetMetrics() (in, out int) {
	return len(w.receive), 0
}
