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

package scanlimiter

import "context"

// Limiter limits concurrent access (e.g. number of parallel scans).
// *semaphore.Weighted from golang.org/x/sync/semaphore implements this interface.
type Limiter interface {
	Acquire(ctx context.Context, n int64) error
	Release(n int64)
	TryAcquire(n int64) bool
}

// noopLimiter is a no-op implementation that never blocks.
type noopLimiter struct{}

func (noopLimiter) Acquire(context.Context, int64) error { return nil }
func (noopLimiter) Release(int64)                        {}
func (noopLimiter) TryAcquire(int64) bool                { return true }

// Noop is a Limiter that does nothing. Use it when no limiting is desired.
var Noop Limiter = noopLimiter{}
