package datahandlers

type JobHandler struct {
	jobs <-chan *DataPipeline
}

func NewJobHandler(jobs <-chan *DataPipeline) *JobHandler {
	return &JobHandler{
		jobs: jobs,
	}
}

// TODO this is not thread safe, the Run method should not be exported
func (jh *JobHandler) Run() error {
	for {
		job, active := <-jh.jobs
		if !active {
			return nil
		}

		err := job.Run()
		if err != nil {
			return err
		}
	}
}
