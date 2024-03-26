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
	"io"
	"runtime/debug"
	"sync/atomic"

	"github.com/aerospike/backup-go/encoding/asb"
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
	if startPartition+numPartitions > MaxPartitions {
		return nil, fmt.Errorf("startPartition + numPartitions is greater than the max partitions count of %d", MaxPartitions)
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

type tokenStats struct {
	records  atomic.Uint64
	sIndexes atomic.Uint32
	uDFs     atomic.Uint32
}

func (bs *tokenStats) GetRecords() uint64 {
	return bs.records.Load()
}

func (bs *tokenStats) GetSIndexes() uint32 {
	return bs.sIndexes.Load()
}

func (bs *tokenStats) GetUDFs() uint32 {
	return bs.uDFs.Load()
}

func (bs *tokenStats) addRecords(num uint64) {
	bs.records.Add(num)
}

func (bs *tokenStats) addSIndexes(num uint32) {
	bs.sIndexes.Add(num)
}

func (bs *tokenStats) addUDFs(num uint32) {
	bs.uDFs.Add(num)
}

func writeASBHeader(w io.Writer, namespace string, first bool) error {
	header, err := asb.GetHeader(namespace, first)
	if err != nil {
		return err
	}

	_, err = w.Write(header)

	return err
}
