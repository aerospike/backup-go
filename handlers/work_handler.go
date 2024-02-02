package handlers

import (
	"context"
	"fmt"
	"log"
	"sync"
)

type Job interface {
	Run() error
}

type jobHandler struct {
	parallel int
	jobs     []Job
	errors   chan error
	wg       *sync.WaitGroup
	workLock *sync.Mutex
}

func NewWorkHandler(jobs []Job, parallel int) *jobHandler {
	return &jobHandler{
		parallel: parallel,
		jobs:     jobs,
		errors:   make(chan error),
		wg:       &sync.WaitGroup{},
		workLock: &sync.Mutex{},
	}
}

func (o *jobHandler) lock() {
	o.workLock.Lock()
}

func (o *jobHandler) unlock() {
	o.workLock.Unlock()
}

func (o *jobHandler) Run() error {
	o.lock()
	defer o.unlock()
	// TODO i should start at 1 if oneshot is done before this
	// TODO maybe these should use a context that cancels all jobs if one fails
	// TODO make sure the single shot work gets done first
	// there are a few cases here
	// all backup jobs finish
	// one or more error
	go func() {
		// TODO allow passing in a context
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// fill the job queue
		jobQueue := make(chan Job, len(o.jobs))
		for _, j := range o.jobs {
			select {
			case <-ctx.Done():
				break
			case jobQueue <- j:
			}
		}
		close(jobQueue)

		for i := 0; i < o.parallel; i++ {
			o.wg.Add(1)
			go func(jobs <-chan Job, ctx context.Context) {
				defer o.wg.Done()

				j := <-jobs

				select {
				case <-ctx.Done():
					return
				default:
					err := j.Run()
					if err != nil {
						log.Println(err) // TODO error logging shouldn't be here
						o.errors <- err
						close(o.errors)
						cancel()
					}
				}
			}(jobQueue, ctx)
		}

		o.wg.Wait()
	}()

	fmt.Println("backup started")

	return nil
}

func (o *jobHandler) Resume() error {
	return o.Run()
}

func (o *jobHandler) Wait() {
	o.wg.Wait()
}
