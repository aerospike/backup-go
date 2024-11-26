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
// Each stage of workers are connected by a single channel that is the size
// of the number of workers in the stage.
// The pipeline stops when a worker returns an error or all workers are done.
// workers should not close the send channel, the pipeline stages handle that.
// Pipelines can be chained together by using them as workers.
type Pipeline[T any] struct {
	stages []*stage[T]
}

const channelSize = 256

// NewPipeline creates a new DataPipeline.
func NewPipeline[T any](routes []Route[T], workGroups ...[]Worker[T]) (*Pipeline[T], error) {
	if len(workGroups) == 0 {
		return nil, fmt.Errorf("workGroups is empty")
	}

	stages := make([]*stage[T], len(workGroups))

	if len(routes) != len(workGroups) {
		return nil, fmt.Errorf("routes and workGroups have different length")
	}

	for i, workers := range workGroups {
		stages[i] = newStage(routes[i], workers...)
	}

	r := newRouter[T]()
	if err := r.apply(stages); err != nil {
		return nil, err
	}

	return &Pipeline[T]{
		stages: stages,
	}, nil
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
	// Is used to stop workers.
	send    []chan T
	workers []Worker[T]
	// Communication routes.
	route Route[T]
}

func (s *stage[T]) SetSendChan(c []chan T) {
	s.send = c
}

func newStage[T any](route Route[T], workers ...Worker[T]) *stage[T] {
	s := stage[T]{
		workers: workers,
		route:   route,
	}

	return &s
}

func (s *stage[T]) Run(ctx context.Context) error {
	if len(s.workers) == 0 {
		return nil
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

	for i := range s.send {
		if s.send[i] != nil {
			close(s.send[i])
		}
	}

	close(errors)

	if len(errors) > 0 {
		return <-errors
	}

	return nil
}
