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
	"errors"
	"io"
)

// dataReader is an interface for reading data from a source.
//
//go:generate mockery --name dataReader
type dataReader[T any] interface {
	Read() (T, error)
	Close()
}

// readWorker implements the pipeline.Worker interface.
// It wraps a DataReader and reads data from it.
type readWorker[T any] struct {
	reader dataReader[T]
	send   chan<- T
}

// NewReadWorker creates a new ReadWorker.
func NewReadWorker[T any](reader dataReader[T]) Worker[T] {
	return &readWorker[T]{
		reader: reader,
	}
}

// SetReceiveChan satisfies the pipeline.Worker interface
// but is a no-op for the ReadWorker.
func (w *readWorker[T]) SetReceiveChan(_ <-chan T) {
	// no-op
}

// SetSendChan sets the send channel for the ReadWorker.
func (w *readWorker[T]) SetSendChan(c chan<- T) {
	w.send = c
}

// Run runs the ReadWorker.
func (w *readWorker[T]) Run(ctx context.Context) error {
	defer w.reader.Close()

	for {
		data, err := w.reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case w.send <- data:
		}
	}
}
