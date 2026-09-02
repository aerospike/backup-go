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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// errJobFailed stands for a failure reported by the backup/restore job itself.
var errJobFailed = errors.New("job failed")

// raceAttempts is the number of times a test repeats a case where several
// select cases are ready at once. A single run would pass by chance, because
// select picks a ready case at random.
const raceAttempts = 100

func TestHandlerBase_WaitForCompletion_JobErrorWinsOverWaitCtx(t *testing.T) {
	t.Parallel()

	for range raceAttempts {
		h := newHandlerBase(t.Context())
		h.errors <- errJobFailed

		waitCtx, cancel := context.WithCancel(t.Context())
		cancel()

		err := h.waitForCompletion(waitCtx)
		require.ErrorIs(t, err, errJobFailed)
	}
}

func TestHandlerBase_WaitForCompletion_JobErrorWinsOverGlobalCtx(t *testing.T) {
	t.Parallel()

	for range raceAttempts {
		ctx, cancel := context.WithCancel(t.Context())

		h := newHandlerBase(ctx)
		h.errors <- errJobFailed

		cancel()

		err := h.waitForCompletion(t.Context())
		require.ErrorIs(t, err, errJobFailed)
	}
}

func TestHandlerBase_WaitForCompletion_CancelWithoutJobError(t *testing.T) {
	t.Parallel()

	h := newHandlerBase(t.Context())

	waitCtx, cancel := context.WithCancel(t.Context())
	cancel()

	err := h.waitForCompletion(waitCtx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestHandlerBase_WaitForCompletion_Success(t *testing.T) {
	t.Parallel()

	h := newHandlerBase(t.Context())
	h.done <- struct{}{}

	require.NoError(t, h.waitForCompletion(t.Context()))
}
