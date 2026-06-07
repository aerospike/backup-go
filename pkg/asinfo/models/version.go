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
	"regexp"
)

var AerospikeVersionRegex = regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)`)

type AerospikeVersion struct {
	Major int
	Minor int
	Patch int
}

var (
	AerospikeVersionSupportsSIndexContext = AerospikeVersion{6, 1, 0}
	AerospikeVersionSupportsBatchWrites   = AerospikeVersion{6, 0, 0}
	// AerospikeVersionRecentInfoCommands after this version, all commands should use
	// `namespace` parameter instead of `ns` or `id`.
	AerospikeVersionRecentInfoCommands = AerospikeVersion{8, 1, 0}

	// AerospikeVersionSupportsIntegratedBackup TODO: change this, after server release.
	AerospikeVersionSupportsIntegratedBackup = AerospikeVersion{8, 1, 0}
)

func (av AerospikeVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", av.Major, av.Minor, av.Patch)
}

func (av AerospikeVersion) IsGreater(other AerospikeVersion) bool {
	if av.Major > other.Major {
		return true
	}

	if av.Major == other.Major {
		if av.Minor > other.Minor {
			return true
		}

		if av.Minor == other.Minor {
			if av.Patch > other.Patch {
				return true
			}
		}
	}

	return false
}

func (av AerospikeVersion) IsGreaterOrEqual(other AerospikeVersion) bool {
	return av.IsGreater(other) || av == other
}
