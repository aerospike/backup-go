package handlers

type workHandler struct {
	currentJob *DataPipeline
}

func newWorkHandler() *workHandler {
	return &workHandler{}
}

func (wh *workHandler) doJob(job *DataPipeline) error {
	wh.currentJob = job
	return wh.currentJob.run()
}
