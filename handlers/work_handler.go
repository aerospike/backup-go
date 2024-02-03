package handlers

import (
	datahandlers "backuplib/data_handlers"
)

type jobHandler struct {
	pipeline *datahandlers.DataPipeline
}

func (jh *jobHandler) Run() error {
	return jh.pipeline.Run()
}

func (jh *jobHandler) Wait() {
	jh.pipeline.Wait()
}
