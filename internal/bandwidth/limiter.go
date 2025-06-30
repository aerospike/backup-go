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

package bandwidth

import (
	"context"

	"github.com/juju/ratelimit"
)

// Limiter wrapper around juju rateLimiter.
type Limiter struct {
	b *ratelimit.Bucket
}

// NewLimiter returns new bandwidth limiter.
func NewLimiter(limit int64) *Limiter {
	if limit > 0 {
		return &Limiter{
			ratelimit.NewBucketWithRate(float64(limit), limit),
		}
	}

	return nil
}

// Wait blocks until lim permits n events to happen.
func (l *Limiter) Wait(ctx context.Context, n int) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	l.b.Wait(int64(n))

	return nil
}
