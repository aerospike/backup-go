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
	"time"
)

// Limiter wrapper around standard rate.Limiter.
type Limiter struct {
	*Bucket
}

// NewLimiter returns new bandwidth limiter.
func NewLimiter(limit int64) (*Limiter, error) {
	if limit > 0 {
		b, err := NewBucket(limit, time.Second)
		if err != nil {
			return nil, err
		}

		return &Limiter{
			b,
		}, nil
	}

	return nil, nil
}

// Wait blocks until limiter permits n events to happen.
func (l *Limiter) Wait(n int) {
	l.Bucket.Wait(int64(n))
}
