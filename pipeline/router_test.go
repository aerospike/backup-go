package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRouter_CreateChannels(t *testing.T) {
	r := NewRouter[int]()

	parallelChannels := r.create(modeParallel, 3, 5)
	assert.Equal(t, 3, len(parallelChannels))
	for _, ch := range parallelChannels {
		assert.NotNil(t, ch)
	}

	singleChannels := r.create(modeSingle, 3, 5)
	assert.Equal(t, 1, len(singleChannels))
	assert.NotNil(t, singleChannels[0])
}

// func TestRouter_SplitChannels(t *testing.T) {
// 	r := NewRouter[int]()
// 	input := make(chan int, 10)
//
// 	go func() {
// 		for i := 0; i < 10; i++ {
// 			input <- i
// 		}
// 		close(input)
// 	}()
//
// 	outputs := r.splitChannels(input, 2, testSplit[int])
// 	assert.Equal(t, 2, len(outputs))
//
// 	wg := sync.WaitGroup{}
// 	wg.Add(2)
//
// 	go func() {
// 		defer wg.Done()
// 		for msg := range outputs[0] {
// 			assert.Equal(t, 0, msg%2)
// 		}
// 	}()
//
// 	go func() {
// 		defer wg.Done()
// 		for msg := range outputs[1] {
// 			assert.Equal(t, 1, msg%2)
// 		}
// 	}()
//
// 	wg.Wait()
// }

func testSplit[T int](v T) int {
	if v%2 == 0 {
		return 0
	}
	return 1
}

func TestRouter_MergeChannels(t *testing.T) {
	r := NewRouter[int]()
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
