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
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func BenchmarkSingleToken(b *testing.B) {
	tests := []struct {
		name     string
		limit    int64
		interval time.Duration
	}{
		{"1000ops/sec", 1000, time.Second},
		{"10000ops/sec", 10000, time.Second},
		{"100ops/ms", 100, time.Millisecond},
		{"1000ops/ms", 1000, time.Millisecond},
	}

	for _, tt := range tests {
		b.Run("CustomBucket_"+tt.name, func(b *testing.B) {
			bucket := NewBucket(tt.limit, tt.interval)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					bucket.Wait(1)
				}
			})
		})

		b.Run("StdLibRate_"+tt.name, func(b *testing.B) {
			limiter := rate.NewLimiter(rate.Limit(float64(tt.limit)/tt.interval.Seconds()), int(tt.limit))
			ctx := context.Background()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					limiter.Wait(ctx)
				}
			})
		})
	}
}

func BenchmarkBatchTokens(b *testing.B) {
	tests := []struct {
		name      string
		limit     int64
		interval  time.Duration
		batchSize int64
	}{
		{"batch10_1000/sec", 1000, time.Second, 10},
		{"batch100_1000/sec", 1000, time.Second, 100},
		{"batch10_10000/sec", 10000, time.Second, 10},
		{"batch100_10000/sec", 10000, time.Second, 100},
	}

	for _, tt := range tests {
		b.Run("CustomBucket_"+tt.name, func(b *testing.B) {
			bucket := NewBucket(tt.limit, tt.interval)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bucket.Wait(tt.batchSize)
			}
		})

		b.Run("StdLibRate_"+tt.name, func(b *testing.B) {
			limiter := rate.NewLimiter(rate.Limit(float64(tt.limit)/tt.interval.Seconds()), int(tt.limit))
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				limiter.WaitN(ctx, int(tt.batchSize))
			}
		})
	}
}

func BenchmarkHighConcurrency(b *testing.B) {
	tests := []struct {
		name       string
		limit      int64
		interval   time.Duration
		goroutines int
	}{
		{"10goroutines_1000/sec", 1000, time.Second, 10},
		{"100goroutines_1000/sec", 1000, time.Second, 100},
		{"10goroutines_10000/sec", 10000, time.Second, 10},
		{"100goroutines_10000/sec", 10000, time.Second, 100},
	}

	for _, tt := range tests {
		b.Run("CustomBucket_"+tt.name, func(b *testing.B) {
			bucket := NewBucket(tt.limit, tt.interval)
			b.ResetTimer()

			var wg sync.WaitGroup
			opsPerGoroutine := b.N / tt.goroutines

			for i := 0; i < tt.goroutines; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < opsPerGoroutine; j++ {
						bucket.Wait(1)
					}
				}()
			}
			wg.Wait()
		})

		b.Run("StdLibRate_"+tt.name, func(b *testing.B) {
			limiter := rate.NewLimiter(rate.Limit(float64(tt.limit)/tt.interval.Seconds()), int(tt.limit))
			ctx := context.Background()
			b.ResetTimer()

			var wg sync.WaitGroup
			opsPerGoroutine := b.N / tt.goroutines

			for i := 0; i < tt.goroutines; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < opsPerGoroutine; j++ {
						limiter.Wait(ctx)
					}
				}()
			}
			wg.Wait()
		})
	}
}

func BenchmarkTryAcquire(b *testing.B) {
	tests := []struct {
		name     string
		limit    int64
		interval time.Duration
	}{
		{"1000ops/sec", 1000, time.Second},
		{"10000ops/sec", 10000, time.Second},
		{"100ops/ms", 100, time.Millisecond},
	}

	for _, tt := range tests {
		b.Run("StdLibRate_"+tt.name, func(b *testing.B) {
			limiter := rate.NewLimiter(rate.Limit(float64(tt.limit)/tt.interval.Seconds()), int(tt.limit))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					limiter.Allow()
				}
			})
		})
	}
}

func BenchmarkTimingAccuracy(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping timing accuracy test in short mode")
	}

	tests := []struct {
		name     string
		limit    int64
		interval time.Duration
	}{
		{"10ops/sec", 10, time.Second},
		{"100ops/sec", 100, time.Second},
		{"1000ops/sec", 1000, time.Second},
	}

	for _, tt := range tests {
		b.Run("CustomBucket_"+tt.name, func(b *testing.B) {
			bucket := NewBucket(tt.limit, tt.interval)
			start := time.Now()

			bucket.Wait(tt.limit)

			bucket.Wait(1)
			elapsed := time.Since(start)

			b.ReportMetric(float64(elapsed.Nanoseconds()), "ns/op")
			b.ReportMetric(float64(elapsed)/float64(tt.interval), "interval_ratio")
		})

		b.Run("StdLibRate_"+tt.name, func(b *testing.B) {
			limiter := rate.NewLimiter(rate.Limit(float64(tt.limit)/tt.interval.Seconds()), int(tt.limit))
			ctx := context.Background()
			start := time.Now()

			for i := 0; i < int(tt.limit); i++ {
				limiter.Wait(ctx)
			}

			limiter.Wait(ctx)
			elapsed := time.Since(start)

			b.ReportMetric(float64(elapsed.Nanoseconds()), "ns/op")
			b.ReportMetric(float64(elapsed)/float64(tt.interval), "interval_ratio")
		})
	}
}

func BenchmarkBurstTraffic(b *testing.B) {
	tests := []struct {
		name       string
		limit      int64
		interval   time.Duration
		burstSize  int64
		burstCount int
	}{
		{"burst10x10_1000/sec", 1000, time.Second, 10, 10},
		{"burst100x10_1000/sec", 1000, time.Second, 100, 10},
		{"burst10x100_1000/sec", 1000, time.Second, 10, 100},
	}

	for _, tt := range tests {
		b.Run("CustomBucket_"+tt.name, func(b *testing.B) {
			bucket := NewBucket(tt.limit, tt.interval)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for j := 0; j < tt.burstCount; j++ {
					bucket.Wait(tt.burstSize)
				}
			}
		})

		b.Run("StdLibRate_"+tt.name, func(b *testing.B) {
			limiter := rate.NewLimiter(rate.Limit(float64(tt.limit)/tt.interval.Seconds()), int(tt.limit))
			ctx := context.Background()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for j := 0; j < tt.burstCount; j++ {
					limiter.WaitN(ctx, int(tt.burstSize))
				}
			}
		})
	}
}
