package handlers

import (
	"context"
	"fmt"
	"log"
	"sync"
)

type Worker interface {
	Run() error
	Resume() error
}

type workHandler struct {
	workers  []Worker
	errors   chan error
	wg       *sync.WaitGroup
	workLock *sync.Mutex
}

func NewWorkHandler(workers []Worker) *workHandler {
	return &workHandler{
		workers:  workers,
		errors:   make(chan error, len(workers)),
		wg:       &sync.WaitGroup{},
		workLock: &sync.Mutex{},
	}
}

func (o *workHandler) lock() {
	o.workLock.Lock()
}

func (o *workHandler) unlock() {
	o.workLock.Unlock()
}

func (o *workHandler) Run() error {
	o.lock()
	// TODO i should start at 1 if oneshot is done before this
	// TODO maybe these should use a context that cancels all jobs if one fails
	// TODO make sure the single shot work gets done first
	// there are a few cases here
	// all backup jobs finish
	// one or more error
	go func() {
		defer o.unlock()
		// TODO allow passing in a context
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		for _, worker := range o.workers {
			o.wg.Add(1)
			go func(w Worker, ctx context.Context) {
				defer o.wg.Done()

				select {
				case <-ctx.Done():
					return
				default:
					err := w.Run()
					if err != nil {
						log.Println(err) // TODO error logging shouldn't be here
						o.errors <- err
						cancel()
					}
				}
			}(worker, ctx)
		}

		o.wg.Wait()
		close(o.errors)
	}()

	fmt.Println("backup started")

	return nil
}

func (o *workHandler) Resume() error {
	return o.Run()
}

func (o *workHandler) Wait() {
	o.wg.Wait()
}
