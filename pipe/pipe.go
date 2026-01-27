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

package pipe

import (
	"context"
	"fmt"
	"sync"

	"github.com/aerospike/backup-go/internal/bandwidth"
	"github.com/aerospike/backup-go/models"
	"golang.org/x/sync/errgroup"
)

// Pipe is running and managing everything.
type Pipe[T models.TokenConstraint] struct {
	readPool  *Pool[T]
	writePool *Pool[T]
	fanout    *Fanout[T]
	// Mutex used to avoid race condition on metrics check after a pipeline was stopped.
	fanMu sync.Mutex
}

// NewPipe creates a new backup/restore pipeline.
func NewPipe[T models.TokenConstraint](
	pc ProcessorCreator[T],
	readers []Reader[T],
	writers []Writer[T],
	limiter *bandwidth.Limiter,
	strategy FanoutStrategy,
) (*Pipe[T], error) {
	readPool := NewReaderPool[T](readers, pc)
	writePool := NewWriterPool[T](writers, limiter)
	// Swap channels!
	// Output of readPool is an input of fanout.
	// Input of writePool is an output of fanout.
	fanout, err := NewFanout[T](readPool.Outputs, writePool.Inputs, strategy)
	if err != nil {
		return nil, fmt.Errorf("failed to create fanout: %w", err)
	}

	return &Pipe[T]{
		readPool:  readPool,
		writePool: writePool,
		fanout:    fanout,
	}, nil
}

// Run starts the pipe with readers, writers and fanout.
func (p *Pipe[T]) Run(ctx context.Context) error {
	errGroup, ctx := errgroup.WithContext(ctx)

	// Fanout runs goroutine for each reader channel that routes messages to writers according to pipe strategy.
	errGroup.Go(func() error {
		p.fanout.Run(ctx)
		return nil
	})

	// Run a readers pool. Each reader in a pool has an output channel, that sends data to fanout.
	errGroup.Go(func() error {
		return p.readPool.Run(ctx)
	})

	// Run a writers pool. Each writer has an input channel in which it receives data to write.
	errGroup.Go(func() error {
		if err := p.writePool.Run(ctx); err != nil {
			fmt.Println("+++++PIPE ERR:", err)
			return err
		}

		return nil
	})

	return errGroup.Wait()
}

// GetMetrics returns the accumulated length for input and output channels.
func (p *Pipe[T]) GetMetrics() (in, out int) {
	// Lock before reading metrics from fanout.
	p.fanMu.Lock()
	defer p.fanMu.Unlock()

	if p.fanout != nil {
		return p.fanout.GetMetrics()
	}

	return 0, 0
}

// Close clean memory for GC.
func (p *Pipe[T]) Close() {
	if p.readPool != nil {
		p.readPool.Close()
		p.readPool = nil
	}

	if p.writePool != nil {
		p.writePool.Close()
		p.writePool = nil
	}
	// Lock before nullifying fanout.
	p.fanMu.Lock()
	defer p.fanMu.Unlock()

	if p.fanout != nil {
		p.fanout = nil
	}
}
