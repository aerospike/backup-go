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

// Limiter wrapper around standard go bandwidth.
type Limiter struct {
	*rate.Limiter
	bandwidth int
}

// NewLimiter returns new bandwidth bandwidth.
func NewLimiter(bandwidth int) *Limiter {
	if bandwidth > 0 {
		return &Limiter{
			rate.NewLimiter(rate.Limit(bandwidth), bandwidth),
			bandwidth,
		}
	}

	return nil
}

// WaitBurst adjusts the limiter burst capacity dynamically and
// blocks until the required tokens are available or context ends.
func (l *Limiter) WaitBurst(ctx context.Context, n int) error {
	// Limiter is initialized with limit and burst == bandwidth.
	// So we check if n is exceeded bandwidth value, we dynamically set burst.
	if n > l.bandwidth {
		l.SetBurst(n)
	}

	return l.WaitN(ctx, n)
}
