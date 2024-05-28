// Copyright 2024-2024 Aerospike, Inc.
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

package processors

import (
	"context"
	"errors"

	"github.com/aerospike/backup-go/models"
)

// **** Processor Worker ****

// DataProcessor is an interface for processing data
//
//go:generate mockery --name DataProcessor
type DataProcessor[T any] interface {
	Process(T) (T, error)
}

type TokenProcessor = DataProcessor[*models.Token]

// ProcessorWorker implements the pipeline.Worker interface
// It wraps a DataProcessor and processes data with it
type ProcessorWorker[T any] struct {
	processor DataProcessor[T]
	receive   <-chan T
	send      chan<- T
}

// NewProcessorWorker creates a new ProcessorWorker
func NewProcessorWorker[T any](processor DataProcessor[T]) *ProcessorWorker[T] {
	return &ProcessorWorker[T]{
		processor: processor,
	}
}

// SetReceiveChan sets receive channel for the ProcessorWorker
func (w *ProcessorWorker[T]) SetReceiveChan(c <-chan T) {
	w.receive = c
}

// SetSendChan sets the send channel for the ProcessorWorker
func (w *ProcessorWorker[T]) SetSendChan(c chan<- T) {
	w.send = c
}

// errFilteredOut is returned by a processor when a token
// should be filtered out of the pipeline
var errFilteredOut = errors.New("filtered out")

// Run starts the ProcessorWorker
func (w *ProcessorWorker[T]) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case data, active := <-w.receive:
			if !active {
				return nil
			}

			processed, err := w.processor.Process(data)
			if errors.Is(err, errFilteredOut) {
				continue
			} else if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case w.send <- processed:
			}
		}
	}
}

// noopProcessor is a no-op implementation of a processor.
type noopProcessor[T any] struct{}

// Process just passes the token through for noopProcessor.
func (n *noopProcessor[T]) Process(token T) (T, error) {
	return token, nil
}
