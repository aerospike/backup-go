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

package backup

import (
	"context"
)

// **** Processor Worker ****

// dataProcessor is an interface for processing data
//
//go:generate mockery --name dataProcessor
type dataProcessor[T any] interface {
	Process(T) (T, error)
}

// processorWorker implements the pipeline.Worker interface
// It wraps a DataProcessor and processes data with it
type processorWorker[T any] struct {
	processor dataProcessor[T]
	receive   <-chan T
	send      chan<- T
}

// newProcessorWorker creates a new ProcessorWorker
func newProcessorWorker[T any](processor dataProcessor[T]) *processorWorker[T] {
	return &processorWorker[T]{
		processor: processor,
	}
}

// SetReceiveChan sets the receive channel for the ProcessorWorker
func (w *processorWorker[T]) SetReceiveChan(c <-chan T) {
	w.receive = c
}

// SetSendChan sets the send channel for the ProcessorWorker
func (w *processorWorker[T]) SetSendChan(c chan<- T) {
	w.send = c
}

// Run starts the ProcessorWorker
func (w *processorWorker[T]) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case data, active := <-w.receive:
			if !active {
				return nil
			}

			processed, err := w.processor.Process(data)
			if err != nil {
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
