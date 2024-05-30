package processors

import (
	"context"
	"testing"
	"time"
)

func TestTPSLimiter(t *testing.T) {
	tests := []struct {
		name string
		tps  int
		runs int
	}{
		{name: "zero tps", tps: 0, runs: 1000},
		{name: "tps 20", tps: 20, runs: 50},
		{name: "tps 500", tps: 500, runs: 2_000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter := NewTPSLimiter[int](context.Background(), tt.tps)

			start := time.Now()
			for i := 0; i < tt.runs; i++ {
				got, err := limiter.Process(i)
				if got != i {
					t.Fatalf("Process() = %v, want %v", got, i)
				}
				if err != nil {
					t.Fatalf("got error while processing token: %v", err)
				}
			}
			duration := time.Since(start)

			const epsilon = 200 * time.Millisecond
			var expectedDuration time.Duration
			if tt.tps > 0 {
				timeRequiredSeconds := float64(tt.runs) / float64(tt.tps)
				expectedDuration = time.Duration(int(timeRequiredSeconds*1000)) * time.Millisecond
			}
			if duration < expectedDuration-epsilon {
				t.Fatalf("Total execution time was too quick, want at least %v, got %v", expectedDuration, duration)
			}
			if duration > expectedDuration+epsilon {
				t.Fatalf("Total execution time was too slow, want at most %v, got %v", expectedDuration, duration)
			}
		})
	}
}
