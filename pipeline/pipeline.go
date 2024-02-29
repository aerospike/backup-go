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

package pipeline

import (
	"context"
	"sync"
)

// Worker is an interface for a pipeline item
// Each worker has a send and receive channel
// that connects it to the previous and next stage in the pipeline
// The Run method starts the worker
//
//go:generate mockery --name Worker
type Worker[T any] interface {
	SetSendChan(chan<- T)
	SetReceiveChan(<-chan T)
	Run(context.Context) error
}

// Pipeline runs a series of workers in parallel.
// All workers run at the same time in separate goroutines.
// Each stage of workers are connected by a single channel that is
// the size of the number of workers in the stage.
// The pipeline stops when a worker returns an error or all workers are done.
// workers should not close the send channel, the pipeline stages handle that.
// Pipelines can be chained together by using them as workers.
type Pipeline[T any] struct {
	receive <-chan T
	send    chan<- T
	stages  []*stage[T]
}

// NewPipeline creates a new DataPipeline.
func NewPipeline[T any](workGroups ...[]Worker[T]) *Pipeline[T] {
	stages := make([]*stage[T], len(workGroups))

	for i, workers := range workGroups {
		stages[i] = newStage(workers...)
	}

	return &Pipeline[T]{
		stages: stages,
	}
}

// SetReceiveChan sets the receive channel for the pipeline.
// The receive channel is used as the input to the first stage.
// This satisfies the Worker interface so that pipelines can be chained together.
// Usually users will not need to call this method.
func (dp *Pipeline[T]) SetReceiveChan(c <-chan T) {
	dp.receive = c
}

// SetSendChan sets the send channel for the pipeline.
// The send channel is used as the output from the last stage.
// This satisfies the Worker interface so that pipelines can be chained together.
// Usually users will not need to call this method.
func (dp *Pipeline[T]) SetSendChan(c chan<- T) {
	dp.send = c
}

// Run starts the pipeline.
// The pipeline stops when a worker returns an error,
// all workers are done, or the context is canceled.
func (dp *Pipeline[T]) Run(ctx context.Context) error {
	if len(dp.stages) == 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errors := make(chan error, len(dp.stages))

	var lastSend chan T

	for i, s := range dp.stages {
		var (
			send chan T
		)

		if i == len(dp.stages)-1 {
			s.SetSendChan(dp.send)
		} else {
			send = make(chan T, len(s.workers))
			s.SetSendChan(send)
		}

		if i == 0 {
			s.SetReceiveChan(dp.receive)
		} else {
			s.SetReceiveChan(lastSend)
		}

		lastSend = send
	}

	wg := &sync.WaitGroup{}
	for _, s := range dp.stages {
		wg.Add(1)

		go func(s *stage[T]) {
			defer wg.Done()

			err := s.Run(ctx)
			if err != nil {
				errors <- err

				cancel()
			}
		}(s)
	}

	wg.Wait()
	close(errors)

	if len(errors) > 0 {
		return <-errors
	}

	return nil
}

type stage[T any] struct {
	receive <-chan T
	send    chan<- T
	workers []Worker[T]
}

func (s *stage[T]) SetReceiveChan(c <-chan T) {
	s.receive = c
}

func (s *stage[T]) SetSendChan(c chan<- T) {
	s.send = c
}

func newStage[T any](workers ...Worker[T]) *stage[T] {
	s := stage[T]{
		workers: workers,
	}

	return &s
}

func (s *stage[T]) Run(ctx context.Context) error {
	if len(s.workers) == 0 {
		return nil
	}

	for _, w := range s.workers {
		w.SetReceiveChan(s.receive)
		w.SetSendChan(s.send)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errors := make(chan error, len(s.workers))

	wg := &sync.WaitGroup{}
	for _, w := range s.workers {
		wg.Add(1)

		go func(w Worker[T]) {
			defer wg.Done()

			err := w.Run(ctx)
			if err != nil {
				errors <- err

				cancel()
			}
		}(w)
	}

	wg.Wait()

	if s.send != nil {
		close(s.send)
	}

	close(errors)

	if len(errors) > 0 {
		return <-errors
	}

	return nil
}
