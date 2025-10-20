package pool

import (
	"testing"
	"time"
)

const defaultPoolSize = 3

func TestPool_Wait(t *testing.T) {
	t.Parallel()
	gPool := NewPool(defaultPoolSize)
	for i := 0; i < 10; i++ {
		taskID := i
		gPool.Submit(func() {
			t.Logf("working on task %d\n", taskID)
			time.Sleep(1 * time.Second)
		})
	}
	gPool.Wait()
	t.Log("done")
}
