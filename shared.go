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
	"log/slog"
	"runtime/debug"
	"sync/atomic"

	"github.com/aerospike/backup-go/encoding/asb"
)

func handlePanic(errors chan<- error, logger *slog.Logger) {
	if r := recover(); r != nil {
		var err error

		panicMsg := "a backup operation has panicked:"
		if _, ok := r.(error); ok {
			err = fmt.Errorf(panicMsg+" caused by this error: \"%w\"\n", r.(error))
		} else {
			err = fmt.Errorf(panicMsg+" caused by: \"%v\"\n", r)
		}

		err = fmt.Errorf("%w, with stacktrace: \"%s\"", err, debug.Stack())
		logger.Error("job failed", "error", err)

		errors <- err
	}
}

func doWork(errors chan<- error, logger *slog.Logger, work func() error) {
	// NOTE: order is important here
	// if we close the errors chan before we handle the panic
	// the panic will attempt to send on a closed channel
	defer close(errors)
	defer handlePanic(errors, logger)

	logger.Info("job starting")

	err := work()
	if err != nil {
		logger.Error("job failed", "error", err)
		errors <- err

		return
	}

	logger.Info("job done")
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
	recordsTotal atomic.Uint64 // number of records read from source
	sIndexes     atomic.Uint32
	uDFs         atomic.Uint32
	totalSize    atomic.Uint64
}

func (bs *tokenStats) GetRecordsTotal() uint64 {
	return bs.recordsTotal.Load()
}

func (bs *tokenStats) GetTotalSize() uint64 {
	return bs.totalSize.Load()
}

func (bs *tokenStats) GetSIndexes() uint32 {
	return bs.sIndexes.Load()
}

func (bs *tokenStats) GetUDFs() uint32 {
	return bs.uDFs.Load()
}

func (bs *tokenStats) addTotalRecords(num uint64) {
	bs.recordsTotal.Add(num)
}

func (bs *tokenStats) addTotalSize(num uint64) {
	bs.totalSize.Add(num)
}

func (bs *tokenStats) addSIndexes(num uint32) {
	bs.sIndexes.Add(num)
}

func (bs *tokenStats) addUDFs(num uint32) {
	bs.uDFs.Add(num)
}

func writeASBHeader(w io.Writer, namespace string, first bool) (int, error) {
	header, err := asb.GetHeader(namespace, first)
	if err != nil {
		return 0, err
	}

	return w.Write(header)
}
