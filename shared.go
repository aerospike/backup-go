// Copyright 2024-2024 Aerospike, Inc.
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

package backup

import (
	"fmt"
	"runtime/debug"
)

func handlePanic(errors chan<- error) {
	if r := recover(); r != nil {
		var err error

		panicMsg := "a backup operation has panicked:"
		if _, ok := r.(error); ok {
			err = fmt.Errorf(panicMsg+" caused by this error: \"%w\"\n", r.(error))
		} else {
			err = fmt.Errorf(panicMsg+" caused by: \"%v\"\n", r)
		}

		err = fmt.Errorf("%w, with stacktrace: \"%s\"", err, debug.Stack())

		errors <- err
	}
}

func splitPartitions(startPartition, numPartitions, numWorkers int) ([]PartitionRange, error) {
	if startPartition+numPartitions > maxPartitions {
		return nil, fmt.Errorf("startPartition + numPartitions is greater than the max partitions count of %d", maxPartitions)
	}

	if numWorkers < 1 {
		return nil, fmt.Errorf("numWorkers is less than 1, cannot split partitions")
	}

	if numPartitions < 1 {
		return nil, fmt.Errorf("numPartitions is less than 1, cannot split partitions")
	}

	if startPartition < 0 {
		return nil, fmt.Errorf("startPartition is less than 0, cannot split partitions")
	}

	pSpecs := make([]PartitionRange, numWorkers)
	for i := 0; i < numWorkers; i++ {
		pSpecs[i].Begin = (i * numPartitions) / numWorkers
		pSpecs[i].Count = (((i + 1) * numPartitions) / numWorkers) - pSpecs[i].Begin
		pSpecs[i].Begin += startPartition
	}

	return pSpecs, nil
}
