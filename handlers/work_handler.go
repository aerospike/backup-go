package handlers

import (
	datahandlers "backuplib/data_handlers"
)

type workHandler struct {
	jobHandler datahandlers.JobHandler
	jobs       <-chan *datahandlers.DataPipeline
	errors     chan<- error
}

func NewWorkHandler(jobs <-chan *datahandlers.DataPipeline, errors chan<- error) *workHandler {
	jh := datahandlers.NewJobHandler(jobs)

	handler := &workHandler{
		jobHandler: *jh,
		jobs:       jobs,
		errors:     errors,
	}

	go func() {
		defer close(errors)
		handler.errors <- handler.jobHandler.Run()
	}()

	return handler
}
