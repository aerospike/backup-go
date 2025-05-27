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
	"errors"
	"fmt"
	"sync"

	"github.com/aerospike/backup-go/models"
	"golang.org/x/time/rate"
)

// Pipe is running and managing everything.
type Pipe[T models.TokenConstraint] struct {
	readPool  *Pool[T]
	writePool *Pool[T]
	fanout    *Fanout[T]
}

// NewPipe creates cew backup pipeline.
func NewPipe[T models.TokenConstraint](
	pc ProcessorCreator[T],
	readers []Reader[T],
	writers []Writer[T],
	limiter *rate.Limiter,
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

// Run start pipe with readers, writers and fanout.
func (p *Pipe[T]) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	errorCh := make(chan error, 3)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(3)

	// Run a readers pool. Each reader in a pool has an output channel, that sends data to fanout.
	go func() {
		defer wg.Done()

		err := p.readPool.Run(ctx)
		if err != nil {
			errorCh <- fmt.Errorf("read pool failed: %w", err)

			cancel()

			return
		}
	}()

	// Run a writers pool. Each writer has an input channel in which it receives data to write.
	go func() {
		defer wg.Done()

		err := p.writePool.Run(ctx)
		if err != nil {
			errorCh <- fmt.Errorf("write pool failed: %w", err)

			cancel()

			return
		}
	}()

	// Fanout runs goroutine for each reader channel that routes messages to writers according to pipe strategy.
	go func() {
		defer wg.Done()
		p.fanout.Run(ctx)
	}()

	wg.Wait()

	// Process errors.
	close(errorCh)

	var errs []error

	for err := range errorCh {
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// GetMetrics returns the accumulated length for input and output channels.
func (p *Pipe[T]) GetMetrics() (in, out int) {
	return p.fanout.GetMetrics()
}
