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
	"path/filepath"
	"strings"
)

// ValidateFilename ensures that the filename is a single, safe path component
// without directory traversal characters, absolute paths, or null bytes.
func ValidateFilename(filename string) error {
	if filename == "" {
		return nil
	}

	if filename == "." {
		return fmt.Errorf("filename must not be '.' (current directory)")
	}

	if filename == ".." {
		return fmt.Errorf("filename must not be '..' (parent directory)")
	}

	if strings.ContainsAny(filename, `/\`) {
		return fmt.Errorf("filename must not contain path separators ('/' or '\\')")
	}

	if strings.ContainsRune(filename, '\x00') {
		return fmt.Errorf("filename must not contain NUL bytes")
	}

	if filepath.IsAbs(filename) {
		return fmt.Errorf("filename must not be an absolute path")
	}

	if filepath.VolumeName(filename) != "" {
		return fmt.Errorf("filename must not contain a Windows volume name or UNC prefix")
	}

	return nil
}
