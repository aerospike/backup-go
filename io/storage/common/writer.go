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
	"path"
)

// GetFullPath returns full path for file or directory, according to params.
func GetFullPath(prefix, filename string, pathList []string, isDir bool) (string, error) {
	if !isDir && len(pathList) == 0 {
		return "", fmt.Errorf("path list can't be empty")
	}

	switch {
	case isDir:
		return path.Join(prefix, filename), nil
	case !isDir && filename != "":
		// If it is a metadata file and we backup to one file.
		return path.Join(path.Dir(pathList[0]), filename), nil
	default:
		// If we use backup to a single file, we overwrite the file name.
		return path.Join(prefix, pathList[0]), nil
	}
}
