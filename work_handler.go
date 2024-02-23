// Copyright 2024-2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backuplib

import datahandlers "github.com/aerospike/aerospike-tools-backup-lib/data_handlers"

// workHandler is a generic worker for running a data pipeline (job)
type workHandler struct{}

func newWorkHandler() *workHandler {
	return &workHandler{}
}

// TODO change the any typed pipeline to a message or token type
func (wh *workHandler) DoJob(job *datahandlers.DataPipeline[any]) error {
	return job.Run()
}
