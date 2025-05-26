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

package pipe

import (
	"context"
	"testing"

	"github.com/aerospike/backup-go/models"
)

func BenchmarkFanoutRouting(b *testing.B) {
	benchmarks := []struct {
		name     string
		strategy FanoutStrategy
		outputs  int
	}{
		{"RoundRobin_2Outputs", RoundRobin, 2},
		{"Straight_2Outputs", Straight, 2},
		{"RoundRobin_4Outputs", RoundRobin, 4},
		{"Straight_4Outputs", Straight, 4},
		{"RoundRobin_8Outputs", RoundRobin, 8},
		{"Straight_8Outputs", Straight, 8},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			outputs := make([]chan *models.Token, bm.outputs)
			for i := range outputs {
				outputs[i] = make(chan *models.Token, b.N)
			}

			inputs := make([]chan *models.Token, bm.outputs)
			for i := range inputs {
				inputs[i] = make(chan *models.Token, 1)
			}

			fanout, err := NewFanout[*models.Token](inputs, outputs, WithStrategy[*models.Token](bm.strategy))
			if err != nil {
				b.Fatalf("Failed to create fanout: %v", err)
			}

			token := &models.Token{
				Type:   models.TokenTypeRecord,
				Record: &models.Record{},
				Size:   10,
			}

			ctx := context.Background()

			// Start goroutines to consume from output channels
			for i := range outputs {
				go func(ch <-chan *models.Token) {
					//nolint:revive //consume the tokens.
					for range ch {
					}
				}(outputs[i])
			}

			// Reset the timer before the benchmark loop
			b.ResetTimer()

			// Benchmark loop
			for i := 0; i < b.N; i++ {
				if bm.strategy == Straight {
					// For Straight strategy, we need to specify an input index
					fanout.routeStraightData(ctx, i%bm.outputs, token)
				} else {
					// For RoundRobin strategy
					fanout.routeRoundRobinData(ctx, token)
				}
			}

			// Stop the timer and clean up
			b.StopTimer()
			fanout.Close()
		})
	}
}

func BenchmarkRouteStraightData(b *testing.B) {
	outputs := make([]chan *models.Token, 4)
	for i := range outputs {
		outputs[i] = make(chan *models.Token, b.N)
	}

	inputs := make([]chan *models.Token, 4)
	for i := range inputs {
		inputs[i] = make(chan *models.Token, 1)
	}

	fanout, err := NewFanout[*models.Token](inputs, outputs, WithStrategy[*models.Token](Straight))
	if err != nil {
		b.Fatalf("Failed to create fanout: %v", err)
	}

	token := &models.Token{
		Type:   models.TokenTypeRecord,
		Record: &models.Record{},
		Size:   10,
	}

	ctx := context.Background()

	for i := range outputs {
		go func(ch <-chan *models.Token) {
			//nolint:revive //consume the tokens.
			for range ch {
			}
		}(outputs[i])
	}

	// Reset the timer before the benchmark loop.
	b.ResetTimer()

	// Benchmark loop.
	for i := 0; i < b.N; i++ {
		fanout.routeStraightData(ctx, i%len(outputs), token)
	}

	// Stop the timer and clean up.
	b.StopTimer()
	fanout.Close()
}

func BenchmarkRouteRoundRobinData(b *testing.B) {
	outputs := make([]chan *models.Token, 4)
	for i := range outputs {
		outputs[i] = make(chan *models.Token, b.N)
	}

	inputs := make([]chan *models.Token, 4)
	for i := range inputs {
		inputs[i] = make(chan *models.Token, 1)
	}

	fanout, err := NewFanout[*models.Token](inputs, outputs, WithStrategy[*models.Token](RoundRobin))
	if err != nil {
		b.Fatalf("Failed to create fanout: %v", err)
	}

	token := &models.Token{
		Type:   models.TokenTypeRecord,
		Record: &models.Record{},
		Size:   10,
	}

	ctx := context.Background()

	for i := range outputs {
		go func(ch <-chan *models.Token) {
			//nolint:revive //consume the tokens.
			for range ch {
			}
		}(outputs[i])
	}

	// Reset the timer before the benchmark loop.
	b.ResetTimer()

	// Benchmark loop.
	for i := 0; i < b.N; i++ {
		fanout.routeRoundRobinData(ctx, token)
	}

	// Stop the timer and clean up.
	b.StopTimer()
	fanout.Close()
}
