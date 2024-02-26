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

import (
	"context"

	"github.com/aerospike/aerospike-tools-backup-lib/pipeline"
)

// workHandler is a generic worker for running a data pipeline (job)
type workHandler struct{}

func newWorkHandler() *workHandler {
	return &workHandler{}
}

func (wh *workHandler) DoJob(job *pipeline.Pipeline[*token]) error {
	// TODO allow for context to be passed in
	ctx := context.Background()
	return job.Run(ctx)
}