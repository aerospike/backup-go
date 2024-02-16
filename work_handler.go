package backuplib

import datahandlers "backuplib/data_handlers"

// workHandler is a generic worker for running a data pipeline (job)
type workHandler struct{}

func newWorkHandler() *workHandler {
	return &workHandler{}
}

func (wh *workHandler) DoJob(job *datahandlers.DataPipeline) error {
	return job.Run()
}
