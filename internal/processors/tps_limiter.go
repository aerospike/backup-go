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

package processors

import (
	"context"

	"github.com/aerospike/backup-go/models"
	"golang.org/x/time/rate"
)

// tpsLimiter is a type representing a Token Per Second limiter.
// it does not allow processing more than tps amount of tokens per second.
type tpsLimiter[T models.TokenConstraint] struct {
	ctx     context.Context
	limiter *rate.Limiter
	tps     int
}

// NewTPSLimiter Create a new TPS limiter.
// n — allowed  number of tokens per second, n = 0 means no limit.
func NewTPSLimiter[T models.TokenConstraint](ctx context.Context, n int) Processor[T] {
	if n == 0 {
		return &noopProcessor[T]{}
	}

	return &tpsLimiter[T]{
		ctx:     ctx,
		tps:     n,
		limiter: rate.NewLimiter(rate.Limit(n), 1),
	}
}

// Process delays pipeline if it's needed to match desired rate.
func (t *tpsLimiter[T]) Process(token T) (T, error) {
	if t.tps == 0 {
		return token, nil
	}

	if err := t.limiter.Wait(t.ctx); err != nil {
		var zero T
		return zero, err
	}

	return token, nil
}
