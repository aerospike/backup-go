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

package aerospike

import (
	"encoding/hex"
	"fmt"

	a "github.com/aerospike/aerospike-client-go/v8"
)

// maxPartitions is the maximum number of partitions in an Aerospike cluster.
const maxPartitions = 4096

// printPartitionFilter return string representation of PartitionFilter
func printPartitionFilter(pf *a.PartitionFilter) string {
	if pf == nil {
		return "<nil>"
	}

	// Special case for "all" partitions
	if pf.Begin == 0 && pf.Count == maxPartitions {
		return "all"
	}

	// If a digest is present, prioritize showing that as it's a specific cursor
	if len(pf.Digest) > 0 {
		return fmt.Sprintf("partition:%d, digest:%s", pf.Begin, hex.EncodeToString(pf.Digest))
	}

	// If it's a single partition
	if pf.Count == 1 {
		return fmt.Sprintf("partition:%d", pf.Begin)
	}

	// Default to start-count notation
	return fmt.Sprintf("range:%d-%d", pf.Begin, pf.Count)
}
