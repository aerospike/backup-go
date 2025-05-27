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

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
	"github.com/segmentio/asm/base64"
	"github.com/stretchr/testify/require"
)

const (
	testBuffer = 100
	testIndex  = 0
)

const (
	testNamespace  = "source-ns1"
	testSetName    = "set1"
	testDigestB64  = "/+Ptyjj06wW9zx0AnxOmq45xJzs="
	testPayloadB64 = "FhABEAAAAAAAAgAnjQAAAAAAAAUAAQAAAAsAc291cmNlLW5zMQAAABUE/+Ptyjj06wW9zx0AnxOmq45xJzsAAAAFAXNldDEAAAAKAgEAAAAAAAADCQAAAAkOAAAAbcndaZgAAAAUAgMAAWF6enp6enp6enp6enp6eno="
)

func testASBXToken() (*models.ASBXToken, error) {
	digest, err := base64.StdEncoding.DecodeString(testDigestB64)
	if err != nil {
		return nil, err
	}

	payload, err := base64.StdEncoding.DecodeString(testPayloadB64)
	if err != nil {
		return nil, err
	}

	k, err := aerospike.NewKeyWithDigest(testNamespace, testSetName, "", digest)
	if err != nil {
		return nil, err
	}

	return models.NewASBXToken(k, payload), nil
}

func TestFanout_Validation(t *testing.T) {
	t.Parallel()

	inputs := make([]chan *models.Token, testParallel)
	outputs := make([]chan *models.Token, testParallel*2)

	fan, err := NewFanout[*models.Token](inputs, outputs, Fixed)
	require.Nil(t, fan)
	require.ErrorContains(t, err, "number for Fixed strategy")

	fan, err = NewFanout[*models.Token](inputs, nil, Split)
	require.Nil(t, fan)
	require.ErrorContains(t, err, "no outputs provided")

	fan, err = NewFanout[*models.Token](nil, outputs, Split)
	require.Nil(t, fan)
	require.ErrorContains(t, err, "no inputs provided")
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

	fan, err := NewFanout[*models.Token](inputs, outputs, RoundRobin)
	require.NoError(t, err)

	// Generate data.
	for i := range inputs {
		go func() {
			n := i
			defer close(inputs[n])
			for range testCount {
				time.Sleep(testDealy)
				inputs[n] <- testToken()
			}
		}()
	}

	// Consume data.
	var (
		counter int
		wg      sync.WaitGroup
	)
	for i := range outputs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n := i
			for range outputs[n] {
				counter++
			}
		}()
	}

	fan.Run(context.Background())

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

	fan, err := NewFanout[*models.Token](inputs, outputs, Fixed)
	require.NoError(t, err)

	// Generate data.
	for i := range inputs {
		go func() {
			n := i
			defer close(inputs[n])
			for range testCount {
				time.Sleep(testDealy)
				inputs[n] <- testToken()
			}
		}()
	}

	// Consume data.
	var (
		counter int
		wg      sync.WaitGroup
	)
	for i := range outputs {
		wg.Add(1)

		go func() {
			defer wg.Done()
			n := i
			for range outputs[n] {
				counter++
			}
		}()
	}

	fan.Run(context.Background())

	wg.Wait()
	// Compare results, after all our calculating routines are finished.
	require.Equal(t, testCount*testParallel, counter)
}

func TestFanout_RunSplit(t *testing.T) {
	t.Parallel()

	inputs := make([]chan *models.ASBXToken, testParallel)
	outputs := make([]chan *models.ASBXToken, testParallel)

	for i := range testParallel {
		inputs[i] = make(chan *models.ASBXToken, testBuffer)
		outputs[i] = make(chan *models.ASBXToken, testBuffer)
	}

	fan, err := NewFanout[*models.ASBXToken](inputs, outputs, Split)
	require.NoError(t, err)

	// Generate data.
	for i := range inputs {
		go func() {
			n := i
			defer close(inputs[n])
			for range testCount {
				time.Sleep(testDealy)
				token, err := testASBXToken()
				require.NoError(t, err)
				inputs[n] <- token
			}
		}()
	}

	// Consume data.
	var (
		counter int
		wg      sync.WaitGroup
	)
	// Count only first output.
	wg.Add(1)
	go func() {
		defer wg.Done()
		// For testASBXToken() index will be 2.
		for range outputs[2] {
			counter++
		}
	}()

	fan.Run(context.Background())

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

	fan, err := NewFanout[*models.Token](inputs, outputs, RoundRobin)
	require.NoError(t, err)

	// Generate data.
	for i := range inputs {
		go func() {
			n := i
			defer close(inputs[n])
			for range testCount {
				time.Sleep(testDealy)
				inputs[n] <- testToken()
			}
		}()
	}

	// Consume data.
	var (
		counter int
		wg      sync.WaitGroup
	)
	for i := range outputs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n := i
			for range outputs[n] {
				counter++
			}
		}()
	}

	ctx, cancel := context.WithCancel(context.Background())
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

	fan, err := NewFanout[*models.Token](inputs, outputs, Fixed)
	require.NoError(t, err)

	// Generate data.
	for i := range inputs {
		go func() {
			n := i
			defer close(inputs[n])
			for range testCount {
				time.Sleep(testDealy)
				inputs[n] <- testToken()
			}
		}()
	}

	// Consume data.
	var (
		counter int
		wg      sync.WaitGroup
	)
	for i := range outputs {
		wg.Add(1)

		go func() {
			defer wg.Done()
			n := i
			for range outputs[n] {
				counter++
			}
		}()
	}

	ctx, cancel := context.WithCancel(context.Background())
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

func TestFanout_RunSplitContextCancel(t *testing.T) {
	t.Parallel()

	inputs := make([]chan *models.Token, testParallel)
	outputs := make([]chan *models.Token, testParallel)

	for i := range testParallel {
		inputs[i] = make(chan *models.Token, testBuffer)
		outputs[i] = make(chan *models.Token, testBuffer)
	}

	fan, err := NewFanout[*models.Token](inputs, outputs, Split)
	require.NoError(t, err)

	// Generate data.
	for i := range inputs {
		go func() {
			n := i
			defer close(inputs[n])
			for j := range testCount {
				time.Sleep(testDealy)
				token := testToken()
				token.Size = uint64(j + i)
				inputs[n] <- token
			}
		}()
	}

	// Consume data.
	var (
		counter int
		wg      sync.WaitGroup
	)
	// Count only first output.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range outputs[testIndex] {
			counter++
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
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
