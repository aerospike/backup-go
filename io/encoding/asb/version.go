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

package asb

import (
	"fmt"
	"strconv"
	"strings"
)

var (
	// versionDefault is the default version of the ASB format.
	versionDefault = newVersion(3, 1)
	// versionExpSindex is the version of the ASB format with expression Sindex support.
	// Should be used only for metadata files.
	versionExpSindex = newVersion(3, 2)

	// Current supported version for decoding.
	versionCurrent = versionExpSindex
)

// version represents protocol version in format major.minor
type version struct {
	Major int
	Minor int
}

// newVersion creates new version.
func newVersion(major, minor int) *version {
	return &version{Major: major, Minor: minor}
}

// parseVersion parses string version.
func parseVersion(v string) (*version, error) {
	parts := strings.Split(v, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid version format: %s", v)
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid major version: %s", parts[0])
	}

	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid minor version: %s", parts[1])
	}

	return &version{Major: major, Minor: minor}, nil
}

// greaterOrEqual checks if current version is greater or equal to other.
func (v *version) greaterOrEqual(other *version) bool {
	return v.compare(other) >= 0
}

func (v *version) compare(other *version) int {
	if v.Major != other.Major {
		if v.Major < other.Major {
			return -1
		}

		return 1
	}

	if v.Minor != other.Minor {
		if v.Minor < other.Minor {
			return -1
		}

		return 1
	}

	return 0
}

func (v *version) toString() string {
	return fmt.Sprintf("%d.%d", v.Major, v.Minor)
}
