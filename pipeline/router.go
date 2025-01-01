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
	"fmt"
	"sync"
)

type routeRuleMode int

const (
	// routeRuleModeParallel each worker get it own channel.
	routeRuleModeParallel routeRuleMode = iota
	// routeRuleModeSingle all workers get one channel.
	routeRuleModeSingle
)

// splitFunc function that processes T and returns the number of a channel to send a message to.
type splitFunc[T any] func(T) int

// Route describes how workers will communicate through stages.
// Each stage must have the corresponding route.
// The Route consists of input and output rule.
type Route[T any] struct {
	input  *RouteRule[T]
	output *RouteRule[T]
}

// NewSingleRoutes helper function, to initialize simple single mode routes.
func NewSingleRoutes[T any](stagesNum int) []Route[T] {
	result := make([]Route[T], 0, stagesNum)

	for i := 0; i < stagesNum; i++ {
		r := Route[T]{
			input:  NewRouteRuleSingle[T](channelSize, nil),
			output: NewRouteRuleSingle[T](channelSize, nil),
		}
		result = append(result, r)
	}

	return result
}

// NewParallelRoutes helper function, to initialize parallel (sync) mode routes.
func NewParallelRoutes[T any](stagesNum int) []Route[T] {
	result := make([]Route[T], 0, stagesNum)

	for i := 0; i < stagesNum; i++ {
		r := Route[T]{
			input:  NewRouteRuleParallel[T](channelSize, nil),
			output: NewRouteRuleParallel[T](channelSize, nil),
		}
		result = append(result, r)
	}

	return result
}

// NewSingleParallelRoutes helper function,
// to initialize routes that will have single mode between first and second stages and
// parallel mode between second and third.
func NewSingleParallelRoutes[T any](sf splitFunc[T]) []Route[T] {
	// This helper is made for XDR support, so stages number is hardcoded.
	result := make([]Route[T], 3)
	// first step.
	result[0] = Route[T]{
		input:  NewRouteRuleSingle[T](channelSize, nil),
		output: NewRouteRuleSingle[T](channelSize, nil),
	}
	// second step.
	result[1] = Route[T]{
		input: NewRouteRuleSingle[T](channelSize, nil),
		// We should set sf function here too, to properly stop it.
		output: NewRouteRuleSingle[T](channelSize, sf),
	}
	// third step with split function on input.
	result[2] = Route[T]{
		input:  NewRouteRuleParallel[T](channelSize, sf),
		output: NewRouteRuleParallel[T](channelSize, nil),
	}

	return result
}

// RouteRule describes how exactly stages will communicate with each other.
type RouteRule[T any] struct {
	// mode can be single or parallel. Depending on that, one or more communication channels will be created.
	mode routeRuleMode
	// bufferSize is applied communication to channels.
	bufferSize int
	// sf split function is used when previous and next step routes have different mode.
	sf splitFunc[T]
	// routedChan is a channel routed by splitFunction.
	// If sf is set, we must close routedChan except worker chan.
	routedChan chan T
}

// NewRouteRuleSingle returns new route rule for single mode communication.
func NewRouteRuleSingle[T any](bufferSize int, sf splitFunc[T]) *RouteRule[T] {
	return &RouteRule[T]{
		mode:       routeRuleModeSingle,
		bufferSize: bufferSize,
		sf:         sf,
	}
}

// NewRouteRuleParallel returns new route rule for parallel mode communication.
func NewRouteRuleParallel[T any](bufferSize int, sf splitFunc[T]) *RouteRule[T] {
	return &RouteRule[T]{
		mode:       routeRuleModeParallel,
		bufferSize: bufferSize,
		sf:         sf,
	}
}

// router is used to route communication channels between stage workers.
// the router is placed between stages.
type router[T any] struct{}

// newRouter returns new router instance.
func newRouter[T any]() *router[T] {
	return &router[T]{}
}

func (r *router[T]) apply(stages []*stage[T]) error {
	// Define channels for a previous step.
	var (
		prevOutput  []chan T
		prevOutMode routeRuleMode
	)
	// For first and last step we need to create empty chan.
	for i, s := range stages {
		// For the first stage, we initialize empty input channels.
		if i == 0 {
			prevOutput = r.create(s.route.input.mode, len(s.workers), 0)
			prevOutMode = s.route.input.mode
		}

		output := make([]chan T, 0)

		switch {
		case prevOutMode == s.route.input.mode:
			// If previous and next modes are the same, we connect workers directly.
			output = r.create(s.route.output.mode, len(s.workers), s.route.output.bufferSize)
		case prevOutMode == routeRuleModeParallel && s.route.input.mode == routeRuleModeSingle:
			// Merge channels.
			op := r.mergeChannels(prevOutput)
			output = append(output, op)
		case prevOutMode == routeRuleModeSingle && s.route.input.mode == routeRuleModeParallel:
			// Split channels.
			if s.route.input.sf == nil {
				return fmt.Errorf("split function not set for stage %d", i)
			}
			// Set to close for the previous stage.
			stages[i-1].route.output.routedChan = prevOutput[0]
			prevOutput = r.splitChannels(prevOutput[0], len(s.workers), s.route.input.sf)

			// output for last step.
			op := make(chan T)
			output = append(output, op)
		}

		if err := r.connect(s, prevOutput, output); err != nil {
			return err
		}

		prevOutput = output
		prevOutMode = s.route.output.mode
	}

	return nil
}

// create creates communication channels.
func (r *router[T]) create(mode routeRuleMode, workersNumber, bufferSize int) []chan T {
	size := workersNumber
	if mode == routeRuleModeSingle {
		size = 1
	}

	result := make([]chan T, 0, size)

	for i := 0; i < size; i++ {
		comChan := make(chan T, bufferSize)
		result = append(result, comChan)
	}

	return result
}

// connect set communication channels to workers.
func (r *router[T]) connect(st *stage[T], input, output []chan T) error {
	st.SetSendChan(output)
	// Set input and output channels.
	for j, w := range st.workers {
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
			// One to many.
			w.SetReceiveChan(input[0])
			w.SetSendChan(output[j])
		default:
			return fmt.Errorf("failed to connect %d workers, with %d input and %d output",
				len(st.workers), len(input), len(output))
		}
	}

	return nil
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

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
