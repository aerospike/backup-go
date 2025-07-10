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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBucket_WaitBig(t *testing.T) {
	t.Parallel()

	const (
		rate  = 7
		limit = 100
	)

	l, err := NewBucket(limit, time.Second)
	require.NoError(t, err)
	tn := time.Now()
	l.Wait(limit * rate)
	ts := time.Since(tn)

	require.InDelta(t, float64(rate), ts.Seconds(), 1)
}

func TestBucket_WaitSmall(t *testing.T) {
	t.Parallel()

	const (
		rate  = 0.2
		limit = 100
	)

	l, err := NewBucket(limit, time.Second)
	require.NoError(t, err)
	tn := time.Now()

	for range 15 {
		l.Wait(limit * rate)
	}

	ts := time.Since(tn)

	require.InDelta(t, 3, ts.Seconds(), 1)
}

func TestBucket_WaitAsync(t *testing.T) {
	t.Parallel()

	const (
		rate  = 0.2
		limit = 100
	)

	l, err := NewBucket(limit, time.Second)
	require.NoError(t, err)
	tn := time.Now()

	var wg sync.WaitGroup

	wg.Add(5)

	for range 5 {
		go func() {
			defer wg.Done()
			for range 5 {
				l.Wait(limit * rate)
			}
		}()
	}

	wg.Wait()
	ts := time.Since(tn)

	require.InDelta(t, 5, ts.Seconds(), 1)
}

func TestBucket_Err(t *testing.T) {
	t.Parallel()

	l, err := NewBucket(-1, time.Second)
	require.Nil(t, l)
	require.ErrorContains(t, err, "limit must be greater than 0")

	l, err = NewBucket(1, -1)
	require.Nil(t, l)
	require.ErrorContains(t, err, "interval must be greater than 0")
}
