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

package pipe

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/require"
)

const (
	testBuffer = 100
)

func TestFanout_Validation(t *testing.T) {
	t.Parallel()

	inputs := make([]chan *models.Token, testParallel)
	outputs := make([]chan *models.Token, testParallel*2)

	fan, err := NewFanout(inputs, outputs, Fixed)
	require.Nil(t, fan)
	require.ErrorContains(t, err, "number for Fixed strategy")
}

func TestFanout_RunDefault(t *testing.T) {
	t.Parallel()

	inputs := make([]chan *models.Token, testParallel)
	outputs := make([]chan *models.Token, testParallel*2)

	for i := range testParallel {
		inputs[i] = make(chan *models.Token, testBuffer)
	}

	for i := range testParallel * 2 {
		outputs[i] = make(chan *models.Token, testBuffer)
	}

	fan, err := NewFanout(inputs, outputs, RoundRobin)
	require.NoError(t, err)

	// Generate data.
	for i := range inputs {
		go func(n int) {
			defer close(inputs[n])
			for range testCount {
				time.Sleep(testDelay)
				inputs[n] <- testToken()
			}
		}(i)
	}

	// Consume data.
	var (
		counter      int
		counterMutex sync.Mutex
		wg           sync.WaitGroup
	)

	for i := range outputs {
		n := i
		wg.Go(func() {
			for range outputs[n] {
				counterMutex.Lock()
				counter++
				counterMutex.Unlock()
			}
		})
	}

	fan.Run(t.Context())

	wg.Wait()
	// Compare results, after all our calculating routines are finished.
	require.Equal(t, testCount*testParallel, counter)
}

func TestFanout_RunStraight(t *testing.T) {
	t.Parallel()

	inputs := make([]chan *models.Token, testParallel)
	outputs := make([]chan *models.Token, testParallel)

	for i := range testParallel {
		inputs[i] = make(chan *models.Token, testBuffer)
		outputs[i] = make(chan *models.Token, testBuffer)
	}

	fan, err := NewFanout(inputs, outputs, Fixed)
	require.NoError(t, err)

	// Generate data.
	for i := range inputs {
		go func(n int) {
			defer close(inputs[n])
			for range testCount {
				time.Sleep(testDelay)
				inputs[n] <- testToken()
			}
		}(i)
	}

	// Consume data.
	var (
		counter      int
		counterMutex sync.Mutex
		wg           sync.WaitGroup
	)
	for i := range outputs {
		n := i
		wg.Go(func() {
			for range outputs[n] {
				counterMutex.Lock()
				counter++
				counterMutex.Unlock()
			}
		})
	}

	fan.Run(t.Context())

	wg.Wait()
	// Compare results, after all our calculating routines are finished.
	require.Equal(t, testCount*testParallel, counter)
}

func TestFanout_RunDefaultContextCancel(t *testing.T) {
	t.Parallel()

	inputs := make([]chan *models.Token, testParallel)
	outputs := make([]chan *models.Token, testParallel*2)

	for i := range testParallel {
		inputs[i] = make(chan *models.Token, testBuffer)
	}

	for i := range testParallel * 2 {
		outputs[i] = make(chan *models.Token, testBuffer)
	}

	fan, err := NewFanout(inputs, outputs, RoundRobin)
	require.NoError(t, err)

	// Generate data.
	for i := range inputs {
		go func(n int) {
			defer close(inputs[n])
			for range testCount {
				time.Sleep(testDelay)
				inputs[n] <- testToken()
			}
		}(i)
	}

	// Consume data.
	var (
		counter      int
		counterMutex sync.Mutex
		wg           sync.WaitGroup
	)
	for i := range outputs {
		n := i
		wg.Go(func() {
			for range outputs[n] {
				counterMutex.Lock()
				counter++
				counterMutex.Unlock()
			}
		})
	}

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(testLongDelay)
		cancel()
	}()

	fan.Run(ctx)

	wg.Wait()
	// Compare results, after all our calculating routines are finished.
	require.Less(t, counter, testCount*testParallel)
	require.Greater(t, counter, testCount)
}

func TestFanout_RunStraightContextCancel(t *testing.T) {
	t.Parallel()

	inputs := make([]chan *models.Token, testParallel)
	outputs := make([]chan *models.Token, testParallel)

	for i := range testParallel {
		inputs[i] = make(chan *models.Token, testBuffer)
		outputs[i] = make(chan *models.Token, testBuffer)
	}

	fan, err := NewFanout(inputs, outputs, Fixed)
	require.NoError(t, err)

	// Generate data.
	for i := range inputs {
		go func(n int) {
			defer close(inputs[n])
			for range testCount {
				time.Sleep(testDelay)
				inputs[n] <- testToken()
			}
		}(i)
	}

	// Consume data.
	var (
		counter      int
		counterMutex sync.Mutex
		wg           sync.WaitGroup
	)
	for i := range outputs {
		n := i
		wg.Go(func() {
			for range outputs[n] {
				counterMutex.Lock()
				counter++
				counterMutex.Unlock()
			}
		})
	}

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(testLongDelay)
		cancel()
	}()

	fan.Run(ctx)

	wg.Wait()
	// Compare results, after all our calculating routines are finished.
	require.Less(t, counter, testCount*testParallel)
	require.Greater(t, counter, testCount)
}
