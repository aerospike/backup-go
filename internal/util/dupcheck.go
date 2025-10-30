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

package util

import "fmt"

// CheckDuplicates checks if a slice of ints or strings contains duplicates.
// It returns an error listing the duplicated values, or nil if all unique.
func CheckDuplicates[T comparable](items []T) error {
	seen := make(map[T]struct{}, len(items))
	dupes := make([]T, 0)

	for _, v := range items {
		if _, exists := seen[v]; exists {
			// append only if not already recorded as duplicate
			already := false

			for _, d := range dupes {
				if d == v {
					already = true
					break
				}
			}

			if !already {
				dupes = append(dupes, v)
			}

			continue
		}

		seen[v] = struct{}{}
	}

	if len(dupes) > 0 {
		return fmt.Errorf("found duplicate values: %v", dupes)
	}

	return nil
}
