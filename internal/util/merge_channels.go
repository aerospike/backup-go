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

package util

import "sync"

// MergeChannels merges multiple channels into a single channel by sending values
// concurrently from each input channel to the output channel.
// The output channel is closed when all the input channels are closed.
func MergeChannels[T any](channels []<-chan T) <-chan T {
	out := make(chan T)

	if len(channels) == 0 {
		close(out)
		return out
	}

	var wg sync.WaitGroup
	// Run an output goroutine for each input channel.
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
