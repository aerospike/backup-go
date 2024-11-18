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
	receive <-chan T
	send    chan<- T
	stages  []*stage[T]
	// For synced pipeline we must create same number of workers for each stage.
	// Then we will initialize communication channels strait from worker to worker through stages.
	isSynced bool
}

var _ Worker[any] = (*Pipeline[any])(nil)

const channelSize = 256

// NewPipeline creates a new DataPipeline.
func NewPipeline[T any](isSynced bool, workGroups ...[]Worker[T]) (*Pipeline[T], error) {
	if len(workGroups) == 0 {
		return nil, fmt.Errorf("workGroups is empty")
	}

	stages := make([]*stage[T], len(workGroups))

	// Check that all working groups have same number of workers.
	if isSynced {
		firstLen := len(workGroups[0])
		for i := range workGroups {
			if len(workGroups[i]) != firstLen {
				return nil, fmt.Errorf("all workers groups must be same length in sync mode")
			}
		}
	}

	for i, workers := range workGroups {
		stages[i] = newStage(isSynced, workers...)
	}

	return &Pipeline[T]{
		stages:   stages,
		isSynced: isSynced,
	}, nil
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

	var (
		lastSend []<-chan T
		// To initialize pipeline workers correctly, we need to create empty channels for first and last stages.
		emptySendChans    []chan<- T
		emptyReceiveChans []<-chan T
	)

	for _, s := range dp.stages {
		sendChans := make([]chan<- T, 0, len(s.workers))
		receiveChans := make([]<-chan T, 0, len(s.workers))

		emptySendChans = make([]chan<- T, 0, len(s.workers))
		emptyReceiveChans = make([]<-chan T, 0, len(s.workers))

		if dp.isSynced {
			for i := 0; i < len(s.workers); i++ {
				// For synced mode, we don't add buffer to channels, not to lose any data.
				send := make(chan T)
				sendChans = append(sendChans, send)
				receiveChans = append(receiveChans, send)

				empty := make(chan T)
				emptySendChans = append(emptySendChans, empty)
				emptyReceiveChans = append(emptyReceiveChans, empty)
			}
		} else {
			send := make(chan T, channelSize)
			sendChans = append(sendChans, send)
			receiveChans = append(receiveChans, send)

			emptySendChans = append(emptySendChans, dp.send)
			emptyReceiveChans = append(emptyReceiveChans, dp.receive)
		}

		s.SetSendChan(sendChans)

		s.SetReceiveChan(lastSend)

		lastSend = receiveChans
	}

	// set the receive and send channels for first
	// and last stages to the pipeline's receive and send channels
	dp.stages[0].SetReceiveChan(emptyReceiveChans)
	dp.stages[len(dp.stages)-1].SetSendChan(emptySendChans)

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
	receive []<-chan T
	send    []chan<- T
	workers []Worker[T]
	// if synced, we distribute communication channels through workers.
	isSynced bool
	// Route rule
	routeRule routeRule
}

func (s *stage[T]) SetReceiveChan(c []<-chan T) {
	s.receive = c
}

func (s *stage[T]) SetSendChan(c []chan<- T) {
	s.send = c
}

func newStage[T any](isSynced bool, workers ...Worker[T]) *stage[T] {
	s := stage[T]{
		workers:  workers,
		isSynced: isSynced,
	}

	return &s
}

func (s *stage[T]) Run(ctx context.Context) error {
	if len(s.workers) == 0 {
		return nil
	}

	for i, w := range s.workers {
		if s.isSynced {
			// We distribute all channels to workers.
			w.SetReceiveChan(s.receive[i])
			w.SetSendChan(s.send[i])

			continue
		}
		// If it is not sync mode, there will be 1 channel in each slice.
		w.SetReceiveChan(s.receive[0])
		w.SetSendChan(s.send[0])
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
