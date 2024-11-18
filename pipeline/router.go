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

import "fmt"

const (
	// ModeOneToOne means that each worker sends messages directly to another stage worker.
	// All stages must have the same number of workers.
	ModeOneToOne = iota
	// ModeManyToOne means that all workers send messages to one channel.
	// And the next stage took messages to process from this one channel.
	// There can be different workers number on each stage.
	ModeManyToOne
	// ModeOneToMany means that one worker sends messages to different workers on next stage.
	ModeOneToMany
)

type routeRule struct {
	Mode      int
	Processor func()
}

type route[T any] struct {
	send []chan T
}

func newRoute[T any](send []chan T) *route[T] {
	return &route[T]{
		send: send,
	}
}

// Router is used to route communication channels between stage workers.
// Router is placed between stages.
type Router[T any] struct {
	// Modes ordered slice of communication modes between stages.
	// For e.g.: for three stages, we must set two modes.
	// For communication between 1 and 2, 2 and 3 stages.
	Modes []int

	// Func for route?
	Rule string
}

// NewRouter returns new Router instance.
func NewRouter[T any](modes []int) *Router[T] {
	return &Router[T]{
		Modes: modes,
	}
}

func (r *Router[T]) Set(stages []*stage[T]) error {
	// For first and last step we need to create empty chan.
	sendChans := make([]chan T, 0, len(stages[0].workers))
	nextRoute := nil
	for i := range stages {
		rt, err := r.applyRoute(stages[i], nextRoute)
		if err != nil {
			return err
		}

		nextRoute = rt
	}

	return nil
}

// ApplyRoute returns route for next stage
func (r *Router[T]) applyRoute(st *stage[T], previousRoute *route[T]) (*route[T], error) {
	// TODO check if previousRoute nil
	switch st.routeRule.Mode {
	case ModeOneToOne:
		if len(st.workers) != len(previousRoute.send) {
			return nil, fmt.Errorf("can't apply route with %d channels to %d workers",
				len(previousRoute.send), len(st.workers))
		}

		sendChans := make([]chan T, 0, len(st.workers))

		for i := range st.workers {
			// Set input.
			st.workers[i].SetReceiveChan(previousRoute.send[i])
			// Set output.
			send := make(chan T)
			st.workers[i].SetSendChan(send)
			sendChans = append(sendChans, send)
		}

		return newRoute(sendChans), nil
	case ModeManyToOne:
		// Set output.
		send := make(chan T)
		for i := range st.workers {
			// Set input.
			st.workers[i].SetReceiveChan(previousRoute.send[i])

			st.workers[i].SetSendChan(send)
		}

		return newRoute([]chan T{send}), nil
	case ModeOneToMany:
		sendChans := make([]chan T, 0, len(st.workers))

		for i := range st.workers {
			// Set input.
			st.workers[i].SetReceiveChan(previousRoute.send[0])
			// Set output.
			send := make(chan T)
			st.workers[i].SetSendChan(send)
			sendChans = append(sendChans, send)
		}

		return newRoute(sendChans), nil
	default:
		return nil, fmt.Errorf("mode %d not supported", st.routeRule.Mode)
	}
}
