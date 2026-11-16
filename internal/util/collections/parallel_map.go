// Copyright 2024-2026 Aerospike, Inc.
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

package collections

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// ParallelMap applies f to every element of in, at most limit at a time, and
// returns the results in the order of in. A limit below one means no limit.
// The first error cancels the context the other calls get, and is returned
// with no results.
func ParallelMap[T, R any](
	ctx context.Context, limit int, in []T, f func(context.Context, T) (R, error),
) ([]R, error) {
	out := make([]R, len(in))

	g, gctx := errgroup.WithContext(ctx)
	if limit > 0 {
		g.SetLimit(limit)
	}

	for i, v := range in {
		g.Go(func() error {
			r, err := f(gctx, v)
			out[i] = r

			return err
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return out, nil
}
