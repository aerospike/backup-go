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

package common

import (
	"fmt"
	"slices"
	"strings"
)

const parentDirName = ".."

// ValidateObjectKey ensures a cloud object key cannot escape its intended prefix.
// '/' is allowed as a key separator. Rejects NUL bytes, keys that start with '/',
// and any path segment equal to '..'.
func ValidateObjectKey(key string) error {
	if key == "" {
		return nil
	}

	if strings.ContainsRune(key, '\x00') {
		return fmt.Errorf("object key must not contain NUL bytes")
	}

	if strings.HasPrefix(key, "/") {
		return fmt.Errorf("object key must not start with '/'")
	}

	if slices.Contains(strings.Split(key, "/"), parentDirName) {
		return fmt.Errorf("object key must not contain '..' path segments")
	}

	return nil
}
