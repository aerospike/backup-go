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

package models

import (
	"fmt"
	"strconv"
)

// InfoMap represents a map of string keys to string values,
// used for parsing response of info requests.
type InfoMap map[string]string

// ParseUint64 returns the parsed uint64 value from the map for the given key if found.
// ok = true if the key was found.
func (m InfoMap) ParseUint64(key string) (result uint64, ok bool, err error) {
	val, ok := m[key]
	if !ok {
		return 0, false, nil
	}

	v, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0, true, fmt.Errorf("failed to parse %s=%q: %w", key, val, err)
	}

	return v, true, nil
}

// ParseInt64 returns the parsed int64 value from the map for the given key if found.
// ok = true if the key was found.
func (m InfoMap) ParseInt64(key string) (result int64, ok bool, err error) {
	val, ok := m[key]
	if !ok {
		return 0, false, nil
	}

	v, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, true, fmt.Errorf("failed to parse %s=%q: %w", key, val, err)
	}

	return v, true, nil
}

// ParseFloat64 returns the parsed float64 value from the map for the given key if found.
// ok = true if the key was found.
func (m InfoMap) ParseFloat64(key string) (result float64, ok bool, err error) {
	val, ok := m[key]
	if !ok {
		return 0, false, nil
	}

	v, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, true, fmt.Errorf("failed to parse %s=%q: %w", key, val, err)
	}

	return v, true, nil
}
