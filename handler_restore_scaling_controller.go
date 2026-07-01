// Copyright 2024 Aerospike, Inc.
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

package backup

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/aerospike/backup-go/models"
)

type restoreScalingController[T models.TokenConstraint] struct {
	rh               *RestoreHandler[T]
	logger           *slog.Logger
	dynamicBatchSize *atomic.Int32
}

func newRestoreScalingController[T models.TokenConstraint](
	rh *RestoreHandler[T],
	dynamicBatchSize *atomic.Int32,
) *restoreScalingController[T] {
	return &restoreScalingController[T]{
		rh:               rh,
		logger:           rh.logger,
		dynamicBatchSize: dynamicBatchSize,
	}
}

func (c *restoreScalingController[T]) run(ctx context.Context) {
	// TODO
}
