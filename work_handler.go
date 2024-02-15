package backuplib

import datahandlers "backuplib/data_handlers"

type workHandler struct{}

func newWorkHandler() *workHandler {
	return &workHandler{}
}

func (wh *workHandler) DoJob(job *datahandlers.DataPipeline) error {
	return job.Run()
}
