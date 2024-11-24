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

import "github.com/aerospike/backup-go/internal/util"

const (
	// modeParallel each worker get it own channel.
	modeParallel = iota
	// modeSingle all workers get one channel.
	modeSingle
)

type splitFunc func(T any) int

type routeRule[T any] struct {
	Mode       int
	BufferSize int
	splitFunc  splitFunc
}

// Router is used to route communication channels between stage workers.
// Router is placed between stages.
type Router[T any] struct {
	// Modes ordered slice of communication modes between stages.
	// For e.g.: for three stages, we must set two modes.
	// For communication between 1 and 2, 2 and 3 stages.
	Rules []routeRule[T]
}

// NewRouter returns new Router instance.
func NewRouter[T any](rules []routeRule[T]) *Router[T] {
	return &Router[T]{
		Rules: rules,
	}
}

func (r *Router[T]) Set(stages []*stage[T]) error {
	// Define channels for a previous step.
	var (
		prevOutput  []chan T
		prevOutMode int
	)
	// For first and last step we need to create empty chan.
	for i, s := range stages {
		// For the first stage, we initialize empty channels.
		if i == 0 {
			prevOutput = r.create(s.inputRoute.Mode, len(s.workers), s.outputRoute.BufferSize)
			prevOutMode = s.outputRoute.Mode
		}

		output := make([]chan T, 0)

		switch {
		case prevOutMode == s.inputRoute.Mode:

			// If previous and next modes are the same, we connect workers directly.
			output = r.create(s.inputRoute.Mode, len(s.workers), s.outputRoute.BufferSize)

		case prevOutMode == modeParallel && s.inputRoute.Mode == modeSingle:
			// Merge channels.
			op := util.MergeChannels(prevOutput)
			output = append(output, op)
		case prevOutMode == modeSingle && s.inputRoute.Mode == modeParallel:
			// Split channels.
			output = r.splitChannels(prevOutput[0], len(s.workers), s.outputRoute.splitFunc)
		}

		r.connect(s.workers, prevOutput, output)

		prevOutput = output
		prevOutMode = s.outputRoute.Mode
	}

	return nil
}

func (r *Router[T]) create(mode, workersNumber, bufferSize int) []chan T {
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

func (r *Router[T]) connect(workers []Worker[T], input, output []chan T) {
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

func (r *Router[T]) splitChannels(commChan chan T, number int, splitFunc splitFunc) []chan T {
	if splitFunc == nil {
		return nil
	}

	out := make([]chan T, 0, number)

	for i := 0; i < number; i++ {
		resChan := make(chan T)
		out = append(out, resChan)
	}

	go func() {
		for msg := range commChan {
			chanNumber := splitFunc(msg)
			out[chanNumber] <- msg
		}
	}()

	return out
}
