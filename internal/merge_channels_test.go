package internal

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
	for _ = range resultChan {
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
