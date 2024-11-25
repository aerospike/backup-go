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
	"sync"
)

const (
	// modeParallel each worker get it own channel.
	modeParallel = iota
	// modeSingle all workers get one channel.
	modeSingle
)

type splitFunc[T any] func(T) int

type Route[T any] struct {
	input  *RouteRule[T]
	output *RouteRule[T]
}

type RouteRule[T any] struct {
	Mode       int
	BufferSize int
	sf         splitFunc[T]
}

func NewRouteRuleSingle[T any](bufferSize int, sf splitFunc[T]) *RouteRule[T] {
	return &RouteRule[T]{
		Mode:       modeSingle,
		BufferSize: bufferSize,
		sf:         sf,
	}
}

func NewRouteRuleParallel[T any](bufferSize int, sf splitFunc[T]) *RouteRule[T] {
	return &RouteRule[T]{
		Mode:       modeParallel,
		BufferSize: bufferSize,
		sf:         sf,
	}
}

// router is used to route communication channels between stage workers.
// the router is placed between stages.
type router[T any] struct{}

// NewRouter returns new router instance.
func NewRouter[T any]() *router[T] {
	return &router[T]{}
}

func (r *router[T]) Set(stages []*stage[T]) error {
	// Define channels for a previous step.
	var (
		prevOutput  []chan T
		prevOutMode int
	)
	// For first and last step we need to create empty chan.
	for i, s := range stages {
		// For the first stage, we initialize empty channels.
		if i == 0 {
			prevOutput = r.create(s.route.input.Mode, len(s.workers), s.route.output.BufferSize)
			prevOutMode = s.route.output.Mode
		}

		output := make([]chan T, 0)

		switch {
		case prevOutMode == s.route.input.Mode:
			// If previous and next modes are the same, we connect workers directly.
			output = r.create(s.route.input.Mode, len(s.workers), s.route.output.BufferSize)
		case prevOutMode == modeParallel && s.route.input.Mode == modeSingle:
			// Merge channels.
			op := r.mergeChannels(prevOutput)
			output = append(output, op)
		case prevOutMode == modeSingle && s.route.input.Mode == modeParallel:
			// Split channels.
			output = r.splitChannels(prevOutput[0], len(s.workers), s.route.output.sf)
		}

		r.connect(s.workers, prevOutput, output)

		prevOutput = output
		prevOutMode = s.route.output.Mode
	}

	return nil
}

func (r *router[T]) create(mode, workersNumber, bufferSize int) []chan T {
	result := make([]chan T, workersNumber)

	switch mode {
	case modeParallel:
		for i := 0; i < workersNumber; i++ {
			comChan := make(chan T, bufferSize)
			result = append(result, comChan)
		}
	case modeSingle:
		comChan := make(chan T, bufferSize)
		result = append(result, comChan)
	}

	return result
}

func (r *router[T]) connect(workers []Worker[T], input, output []chan T) {
	// Set input and output channels.
	for j, w := range workers {
		switch {
		case len(input) == 1 && len(output) == 1:
			// Single.
			w.SetReceiveChan(input[0])
			w.SetSendChan(output[0])
		case len(input) == len(output):
			// Parallel.
			w.SetReceiveChan(input[j])
			w.SetSendChan(output[j])
		case len(input) > len(output) && len(output) == 1:
			// Many to one.
			w.SetReceiveChan(input[j])
			w.SetSendChan(output[0])
		case len(input) < len(output) && len(input) == 1:
			// One to many
			w.SetReceiveChan(input[0])
			w.SetSendChan(output[j])
		}
	}
}

func (r *router[T]) splitChannels(commChan chan T, number int, sf splitFunc[T]) []chan T {
	if sf == nil {
		return nil
	}

	out := make([]chan T, 0, number)

	for i := 0; i < number; i++ {
		resChan := make(chan T)
		out = append(out, resChan)
	}

	go func() {
		for msg := range commChan {
			chanNumber := sf(msg)
			out[chanNumber] <- msg
		}
		for i := 0; i < number; i++ {
			close(out[i])
		}
	}()

	return out
}

func (r *router[T]) mergeChannels(channels []chan T) chan T {
	out := make(chan T)

	if len(channels) == 0 {
		close(out)
		return out
	}

	var wg sync.WaitGroup

	output := func(c <-chan T) {
		for n := range c {
			out <- n
		}

		wg.Done()
	}

	wg.Add(len(channels))

	for _, c := range channels {
		go output(c)
	}

	// Run a goroutine to close out once all the output goroutines are done.
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
