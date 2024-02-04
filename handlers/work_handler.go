package handlers

import (
	datahandlers "backuplib/data_handlers"
)

// TODO move job handler to the datahandlers package
// don't export DataPipeline's Run method after that
// just have the job handler call the Run method
// then make a job handler a private field of the handler
// (not a pointer, just a value) so that each handler has its own job handler
type jobHandler struct {
	pipeline *datahandlers.DataPipeline
}

// TODO this is not thread safe, the Run method should not be exported
func (jh *jobHandler) Run(job *datahandlers.DataPipeline) error {
	jh.pipeline = job
	return job.Run()
}

func (jh *jobHandler) Wait() {
	jh.pipeline.Wait()
}
