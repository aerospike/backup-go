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

import (
	"testing"
)

func TestMergeMultipleChannels(t *testing.T) {
	ch1 := make(chan int)
	ch2 := make(chan int)
	ch3 := make(chan int)

	go func() {
		for i := 0; i < 5; i++ {
			ch1 <- i
			ch2 <- i
			ch3 <- i
		}
		close(ch1)
		close(ch2)
		close(ch3)
	}()

	resultChan := MergeChannels([]<-chan int{ch1, ch2, ch3})

	counter := 0
	for range resultChan {
		counter++
	}
	if counter != 15 {
		t.Errorf("Expected 15 results, but got %v", counter)
	}
}

func TestMergeNoChannels(t *testing.T) {
	resultChan := MergeChannels([]<-chan int{}) // Merge with no channels

	if _, ok := <-resultChan; ok {
		t.Errorf("Expected channel to be closed, but it was open")
	}
}

func TestMergeOneChannel(t *testing.T) {
	ch1 := make(chan int)
	go func() {
		ch1 <- 10
		close(ch1)
	}()

	resultChan := MergeChannels([]<-chan int{ch1})
	res := <-resultChan

	if res != 10 {
		t.Errorf("Expected 10, but got %v", res)
	}
}
