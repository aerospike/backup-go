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
	"testing"
	"time"

	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/require"
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
			limiter := NewTPSLimiter[*models.Token](t.Context(), tt.tps)

			start := time.Now()
			for i := 0; i < tt.runs; i++ {
				token := models.NewRecordToken(nil, 1, nil)
				got, err := limiter.Process(token)
				require.Same(t, token, got, "process should return the same token instance")
				require.NoError(t, err, "process should not fail")
			}
			duration := time.Since(start)

			const minEpsilon = 200 * time.Millisecond
			const maxEpsilon = 300 * time.Millisecond
			var expectedDuration time.Duration
			if tt.tps > 0 {
				// rate.Limiter with burst=1 allows the first token immediately.
				timeRequiredSeconds := float64(tt.runs-1) / float64(tt.tps)
				expectedDuration = time.Duration(int(timeRequiredSeconds*1000)) * time.Millisecond
			}
			require.GreaterOrEqual(
				t,
				duration,
				expectedDuration-minEpsilon,
				"total execution time was too quick",
			)
			require.LessOrEqual(
				t,
				duration,
				expectedDuration+maxEpsilon,
				"total execution time was too slow",
			)
		})
	}
}
