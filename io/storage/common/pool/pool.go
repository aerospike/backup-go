package pool

import "sync"

// Pool simple pool for goroutines.
type Pool struct {
	workers  int
	wg       sync.WaitGroup
	workChan chan struct{}
}

// NewPool returns new goroutine pool.
func NewPool(workers int) *Pool {
	p := &Pool{
		workers:  workers,
		workChan: make(chan struct{}, workers),
	}

	return p
}

// Submit adds new goroutine to pool.
func (p *Pool) Submit(f func()) {
	// Adding one to a waiting group.
	p.wg.Add(1)
	// Adding empty field to chanel to take one worker place.
	p.workChan <- struct{}{}
	go func() {
		// When function will finish it's work we release a waiting group.
		defer p.wg.Done()
		// Run our goroutine.
		f()
		// Remove message from channel to release space for new worker.
		<-p.workChan
	}()
}

// Wait waits till all goroutines will be finished.
func (p *Pool) Wait() {
	p.wg.Wait()
}
