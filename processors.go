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

package backuplib

import "context"

// processors.go contains the implementations of the DataProcessor interface
// used by dataPipelines in the backuplib package

// **** Processor Worker ****

// DataProcessor is an interface for processing data
//
//go:generate mockery --name DataProcessor
type DataProcessor[T any] interface {
	Process(T) (T, error)
}

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

// SetReceiveChan sets the receive channel for the ProcessorWorker
func (w *ProcessorWorker[T]) SetReceiveChan(c <-chan T) {
	w.receive = c
}

// SetSendChan sets the send channel for the ProcessorWorker
func (w *ProcessorWorker[T]) SetSendChan(c chan<- T) {
	w.send = c
}

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

// **** NOOP Processor ****

// NOOPProcessor satisfies the DataProcessor interface
// It does nothing to the data
type NOOPProcessor struct{}

// NewNOOPProcessor creates a new NOOPProcessor
func NewNOOPProcessor() *NOOPProcessor {
	return &NOOPProcessor{}
}

// Process processes the data
func (p *NOOPProcessor) Process(data *token) (*token, error) {
	return data, nil
}

type NOOPProcessorFactory struct{}
