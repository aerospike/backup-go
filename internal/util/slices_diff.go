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

// Diff finds the difference between two slices by returning elements from
// s1 that are not in s2.
func Diff[S ~[]E, E comparable](s1, s2 S) S {
	// Fill the map.
	m2 := make(map[E]struct{}, len(s2))
	for _, v := range s2 {
		m2[v] = struct{}{}
	}

	res := make(S, 0, len(s1))

	for _, v := range s1 {
		if _, exists := m2[v]; !exists {
			res = append(res, v)
		}
	}

	return res
}
