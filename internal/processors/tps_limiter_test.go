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
	"testing"
	"time"

	"github.com/aerospike/backup-go/models"
)

func TestTPSLimiter(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			limiter := NewTPSLimiter[*models.Token](context.Background(), tt.tps)

			start := time.Now()
			for i := 0; i < tt.runs; i++ {
				token := models.NewRecordToken(nil, 1, nil)
				got, err := limiter.Process(token)
				if got != token {
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
