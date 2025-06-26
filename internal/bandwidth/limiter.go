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

	"golang.org/x/time/rate"
)

const (
	// DefaultLimit is 8 mb as aerospike record size is limited with 8mb.
	DefaultLimit = 8 * 1024 * 1024
	// metaOverhead approximate size of record's metadata: namespace, set name, key, etc.
	metaOverhead = 10_000
)

// Limiter wrapper around standard go bandwidth.
type Limiter struct {
	*rate.Limiter
	bandwidth int
}

// NewLimiter returns new bandwidth limiter.
func NewLimiter(limit int) *Limiter {
	if limit > 0 {
		bandwidth := DefaultLimit + metaOverhead
		if limit > bandwidth {
			bandwidth = limit
		}

		return &Limiter{
			rate.NewLimiter(rate.Limit(bandwidth), bandwidth),
			bandwidth,
		}
	}

	return nil
}

// Wait blocks until lim permits n events to happen.
func (l *Limiter) Wait(ctx context.Context, n int) error {
	return l.WaitN(ctx, n)
}
