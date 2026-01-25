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

package files

import (
	"fmt"
	"strconv"
	"strings"
)

// GetFileNumber returns file number from name.
// Return 0 nil for non-asbx files
func GetFileNumber(filename string) (uint64, error) {
	// Skip non asbx files.
	if !strings.HasSuffix(filename, ExtensionASBX) {
		return 0, nil
	}

	name := strings.TrimSuffix(filename, ExtensionASBX)
	parts := strings.SplitN(name, "_", 3)

	if len(parts) != 3 {
		return 0, fmt.Errorf("invalid file name %q", filename)
	}

	num, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse file number %q: %w", filename, err)
	}

	return num, nil
}
