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
