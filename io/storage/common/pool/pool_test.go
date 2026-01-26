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

package pool

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const defaultPoolSize = 3

func TestPool_Wait(t *testing.T) {
	t.Parallel()
	gPool := NewPool(defaultPoolSize)
	for i := range 10 {
		taskID := i
		gPool.Submit(func() {
			t.Logf("working on task %d\n", taskID)
			time.Sleep(1 * time.Second)
		})
	}
	gPool.Wait()
	t.Log("done")
}

func TestPool_Submit_NilPool(t *testing.T) {
	var p *Pool

	executed := false
	p.Submit(func() {
		executed = true
	})

	assert.True(t, executed, "function should execute synchronously")
}

func TestPool_RaceCondition(t *testing.T) {
	const (
		workers = 10
		tasks   = 10000
	)

	p := NewPool(workers)
	require.NotNil(t, p)

	var counter atomic.Int32
	for range tasks {
		p.Submit(func() {
			counter.Add(1)
		})
	}

	p.Wait()
	assert.Equal(t, int32(tasks), counter.Load())
}
