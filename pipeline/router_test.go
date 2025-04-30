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
	"testing"

	"github.com/aerospike/backup-go/pipeline/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestRouter_CreateChannels(t *testing.T) {
	r := newRouter[int]()

	parallelChannels := r.create(routeRuleModeParallel, 3, 5)
	assert.Equal(t, 3, len(parallelChannels))
	for _, ch := range parallelChannels {
		assert.NotNil(t, ch)
	}

	singleChannels := r.create(routeRuleModeSingle, 3, 5)
	assert.Equal(t, 1, len(singleChannels))
	assert.NotNil(t, singleChannels[0])
}

func TestRouter_SplitChannels(t *testing.T) {
	r := newRouter[int]()
	input := make(chan int, 10)

	go func() {
		for i := 0; i < 10; i++ {
			input <- i
		}
		close(input)
	}()

	outputs := r.splitChannels(input, 2, testSplit[int])
	assert.Equal(t, 2, len(outputs))

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		for msg := range outputs[0] {
			assert.Equal(t, 0, msg%2)
		}
	}()

	go func() {
		defer wg.Done()
		for msg := range outputs[1] {
			assert.Equal(t, 1, msg%2)
		}
	}()

	wg.Wait()
}

func testSplit[T int](v T) int {
	if v%2 == 0 {
		return 0
	}
	return 1
}

func TestRouter_MergeChannels(t *testing.T) {
	r := newRouter[int]()
	ch1 := make(chan int, 5)
	ch2 := make(chan int, 5)

	for i := 0; i < 5; i++ {
		ch1 <- i
		ch2 <- i + 10
	}
	close(ch1)
	close(ch2)

	merged := r.mergeChannels([]chan int{ch1, ch2})
	result := make([]int, 0)

	for msg := range merged {
		result = append(result, msg)
	}

	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 10, 11, 12, 13, 14}, result)
}

func TestRouterApply(t *testing.T) {
	t.Run("SingleModeThroughout", func(t *testing.T) {
		r := newRouter[string]()

		worker1 := mocks.NewMockWorker[string](t)
		worker1.EXPECT().SetReceiveChan(mock.Anything)
		worker1.EXPECT().SetSendChan(mock.Anything)

		worker2 := mocks.NewMockWorker[string](t)
		worker2.EXPECT().SetReceiveChan(mock.Anything)
		worker2.EXPECT().SetSendChan(mock.Anything)

		stage1 := newStage(Route[string]{
			input:  NewRouteRuleSingle[string](channelSize, nil),
			output: NewRouteRuleSingle[string](channelSize, nil),
		}, worker1)

		stage2 := newStage(Route[string]{
			input:  NewRouteRuleSingle[string](channelSize, nil),
			output: NewRouteRuleSingle[string](channelSize, nil),
		}, worker2)

		err := r.apply([]*stage[string]{stage1, stage2})
		assert.NoError(t, err)
	})

	t.Run("ParallelModeThroughout", func(t *testing.T) {
		r := newRouter[string]()

		worker1 := mocks.NewMockWorker[string](t)
		worker1.EXPECT().SetReceiveChan(mock.Anything)
		worker1.EXPECT().SetSendChan(mock.Anything)

		worker2 := mocks.NewMockWorker[string](t)
		worker2.EXPECT().SetReceiveChan(mock.Anything)
		worker2.EXPECT().SetSendChan(mock.Anything)

		stage1 := newStage(Route[string]{
			input:  NewRouteRuleParallel[string](channelSize, nil),
			output: NewRouteRuleParallel[string](channelSize, nil),
		}, worker1)

		stage2 := newStage(Route[string]{
			input:  NewRouteRuleParallel[string](channelSize, nil),
			output: NewRouteRuleParallel[string](channelSize, nil),
		}, worker2)

		err := r.apply([]*stage[string]{stage1, stage2})
		assert.NoError(t, err)
	})

	t.Run("ParallelToSingleMode", func(t *testing.T) {
		r := newRouter[string]()

		worker1 := mocks.NewMockWorker[string](t)
		worker1.EXPECT().SetReceiveChan(mock.Anything)
		worker1.EXPECT().SetSendChan(mock.Anything)

		worker2 := mocks.NewMockWorker[string](t)
		worker2.EXPECT().SetReceiveChan(mock.Anything)
		worker2.EXPECT().SetSendChan(mock.Anything)

		stage1 := newStage(Route[string]{
			input:  NewRouteRuleParallel[string](channelSize, nil),
			output: NewRouteRuleParallel[string](channelSize, nil),
		}, worker1, worker2)

		stage2 := newStage(Route[string]{
			input:  NewRouteRuleSingle[string](channelSize, nil),
			output: NewRouteRuleSingle[string](channelSize, nil),
		}, worker1)

		err := r.apply([]*stage[string]{stage1, stage2})
		assert.NoError(t, err)
	})

	t.Run("SingleToParallelMode", func(t *testing.T) {
		r := newRouter[string]()

		sf := func(v string) int {
			if v == "test" {
				return 0
			}
			return 1
		}

		worker1 := mocks.NewMockWorker[string](t)
		worker1.EXPECT().SetReceiveChan(mock.Anything)
		worker1.EXPECT().SetSendChan(mock.Anything)

		worker2 := mocks.NewMockWorker[string](t)
		worker2.EXPECT().SetReceiveChan(mock.Anything)
		worker2.EXPECT().SetSendChan(mock.Anything)

		stage1 := newStage(Route[string]{
			input:  NewRouteRuleSingle[string](channelSize, nil),
			output: NewRouteRuleSingle[string](channelSize, sf),
		}, worker1)

		stage2 := newStage(Route[string]{
			input:  NewRouteRuleParallel[string](channelSize, sf),
			output: NewRouteRuleParallel[string](channelSize, nil),
		}, worker1, worker2)

		err := r.apply([]*stage[string]{stage1, stage2})
		assert.NoError(t, err)
	})

	t.Run("SplitChannelsWithNilSplitFunction", func(t *testing.T) {
		r := newRouter[string]()

		result := r.splitChannels(make(chan string), 2, nil)
		assert.Nil(t, result, "Expected nil result when split function is nil")
	})

	t.Run("ErrorInvalidConnection", func(t *testing.T) {
		r := newRouter[string]()

		worker1 := mocks.NewMockWorker[string](t)
		worker2 := mocks.NewMockWorker[string](t)

		stage2 := &stage[string]{
			workers: []Worker[string]{worker1, worker2},
			route: Route[string]{
				input:  NewRouteRuleParallel[string](channelSize, nil),
				output: NewRouteRuleParallel[string](channelSize, nil),
			},
		}

		input := []chan string{make(chan string), make(chan string)}
		output := []chan string{make(chan string), make(chan string), make(chan string)} // More outputs than workers

		err := r.connect(stage2, input, output)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to connect")
	})
}

func TestRouterMergeChannelsEmptyInput(t *testing.T) {
	r := newRouter[string]()

	result := r.mergeChannels([]chan string{})

	_, ok := <-result
	assert.False(t, ok, "Expected channel to be closed")
}

func TestNewParallelRoutes(t *testing.T) {
	routes := NewParallelRoutes[string](3)

	assert.Equal(t, 3, len(routes), "Expected 3 routes")

	for i, route := range routes {
		assert.Equal(t, routeRuleModeParallel, route.input.mode, "Expected parallel mode for input of route %d", i)
		assert.Nil(t, route.input.sf, "Expected nil split function for input of route %d", i)
		assert.Equal(t, 256, route.input.bufferSize, "Expected buffer size 256 for input of route %d", i)

		assert.Equal(t, routeRuleModeParallel, route.output.mode, "Expected parallel mode for output of route %d", i)
		assert.Nil(t, route.output.sf, "Expected nil split function for output of route %d", i)
		assert.Equal(t, 256, route.output.bufferSize, "Expected buffer size 256 for output of route %d", i)
	}
}

func TestNewSingleParallelRoutes(t *testing.T) {
	sf := func(v string) int {
		if v == "test" {
			return 0
		}
		return 1
	}

	routes := NewSingleParallelRoutes(sf)

	assert.Equal(t, 3, len(routes), "Expected 3 routes")

	assert.Equal(t, routeRuleModeSingle, routes[0].input.mode, "Expected single mode for input of first route")
	assert.Nil(t, routes[0].input.sf, "Expected nil split function for input of first route")
	assert.Equal(t, 256, routes[0].input.bufferSize, "Expected buffer size 256 for input of first route")

	assert.Equal(t, routeRuleModeSingle, routes[0].output.mode, "Expected single mode for output of first route")
	assert.Nil(t, routes[0].output.sf, "Expected nil split function for output of first route")
	assert.Equal(t, 256, routes[0].output.bufferSize, "Expected buffer size 256 for output of first route")

	assert.Equal(t, routeRuleModeSingle, routes[1].input.mode, "Expected single mode for input of second route")
	assert.Nil(t, routes[1].input.sf, "Expected nil split function for input of second route")
	assert.Equal(t, 256, routes[1].input.bufferSize, "Expected buffer size 256 for input of second route")

	assert.Equal(t, routeRuleModeSingle, routes[1].output.mode, "Expected single mode for output of second route")
	assert.NotNil(t, routes[1].output.sf, "Expected non-nil split function for output of second route")
	assert.Equal(t, 256, routes[1].output.bufferSize, "Expected buffer size 256 for output of second route")

	assert.Equal(t, routeRuleModeParallel, routes[2].input.mode, "Expected parallel mode for input of third route")
	assert.NotNil(t, routes[2].input.sf, "Expected non-nil split function for input of third route")
	assert.Equal(t, 256, routes[2].input.bufferSize, "Expected buffer size 256 for input of third route")

	assert.Equal(t, routeRuleModeParallel, routes[2].output.mode, "Expected parallel mode for output of third route")
	assert.Nil(t, routes[2].output.sf, "Expected nil split function for output of third route")
	assert.Equal(t, 256, routes[2].output.bufferSize, "Expected buffer size 256 for output of third route")

	assert.Equal(t, 0, routes[2].input.sf("test"), "Expected split function to return 0 for 'test'")
	assert.Equal(t, 1, routes[2].input.sf("other"), "Expected split function to return 1 for 'other'")
}

func TestNewRouteRuleParallel(t *testing.T) {
	sf := func(v string) int {
		if v == "test" {
			return 0
		}
		return 1
	}

	rule := NewRouteRuleParallel(200, sf)

	assert.Equal(t, routeRuleModeParallel, rule.mode, "Expected parallel mode")
	assert.NotNil(t, rule.sf, "Expected a non-nil split function")
	assert.Equal(t, 200, rule.bufferSize, "Expected buffer size 200")

	assert.Equal(t, 0, rule.sf("test"), "Expected split function to return 0 for 'test'")
	assert.Equal(t, 1, rule.sf("other"), "Expected split function to return 1 for 'other'")
}
