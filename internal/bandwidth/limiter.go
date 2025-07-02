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
	"time"
)

const (
	// MinLimit represents the minimum allowed bandwidth, constrained by the maximum record size (8 Mb).
	MinLimit = 8 * 1024 * 1024
	// metaOverhead represents an approximate size of record's metadata: namespace, set name, key, etc.
	metaOverhead = 16 * 1024
	// base64Ratio defines the multiplier to account for size expansion when encoding data using Base64.
	base64Ratio = 1.34
)

// Limiter wrapper around standard rate.Limiter.
type Limiter struct {
	*Bucket
	bandwidth int
}

// NewLimiter returns new bandwidth limiter.
func NewLimiter(limit int) *Limiter {
	if limit > 0 {
		// bandwidth := newBandwidth(limit)
		return &Limiter{
			NewBucket(int64(limit), time.Second),
			limit,
		}
	}

	return nil
}

// Wait blocks until lim permits n events to happen.
func (l *Limiter) Wait(ctx context.Context, n int) error {
	l.Bucket.Wait(int64(n))

	return nil
}

// newBandwidth returns a calculated value for bandwidth.
func newBandwidth(limit int) int {
	bandwidth := MinLimit
	if limit > bandwidth {
		bandwidth = limit
	}

	bandwidth = int(float64(bandwidth)*base64Ratio) + metaOverhead

	return bandwidth
}
