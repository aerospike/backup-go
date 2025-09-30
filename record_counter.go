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

package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/io/aerospike"
	"github.com/aerospike/backup-go/models"
)

// recordCounter contains logic to calculate approximate records count.
// Notice! At the moment works only with *models.Token type.
type recordCounter struct {
	aerospikeClient AerospikeClient
	infoClient      InfoGetter
	config          *ConfigBackup
	readerProcessor *recordReaderProcessor[*models.Token]

	logger *slog.Logger
}

func newRecordCounter(
	aerospikeClient AerospikeClient,
	infoClient InfoGetter,
	config *ConfigBackup,
	readerProcessor *recordReaderProcessor[*models.Token],
	logger *slog.Logger,
) *recordCounter {
	return &recordCounter{
		aerospikeClient: aerospikeClient,
		infoClient:      infoClient,
		config:          config,
		readerProcessor: readerProcessor,
		logger:          logger,
	}
}

func (rc *recordCounter) countRecords(ctx context.Context, infoClient InfoGetter) (uint64, error) {
	if rc.config.withoutFilter() {
		return rc.countUsingInfoClient(infoClient)
	}

	return rc.countRecordsUsingScan(ctx)
}

func (rc *recordCounter) countUsingInfoClient(infoClient InfoGetter) (uint64, error) {
	totalRecordCount, err := infoClient.GetRecordCount(rc.config.Namespace, rc.config.SetList)
	if err != nil {
		return 0, fmt.Errorf("failed to get record count: %w", err)
	}

	partitionsToScan := uint64(sumPartition(rc.config.PartitionFilters))

	return totalRecordCount * partitionsToScan / MaxPartitions, nil
}

func (rc *recordCounter) countRecordsUsingScan(ctx context.Context) (uint64, error) {
	scanPolicy := *rc.config.ScanPolicy

	scanPolicy.IncludeBinData = false
	scanPolicy.MaxRecords = 0

	var err error

	filters := rc.config.PartitionFilters

	if rc.config.isProcessedByNodes() {
		filters, err = rc.readerProcessor.newPartitionGroupsFromNodes(rc.config.ParallelRead)
		if err != nil {
			return 0, fmt.Errorf("failed to create partition groups: %w", err)
		}
	}

	return rc.countRecordsUsingScanByPartitions(ctx, &scanPolicy, filters)
}

func (rc *recordCounter) countRecordsUsingScanByPartitions(
	ctx context.Context, scanPolicy *a.ScanPolicy, filters []*a.PartitionFilter,
) (uint64, error) {
	var count uint64

	partitionFilter := randomPartition(filters)
	readerConfig := rc.readerProcessor.newRecordReaderConfig(partitionFilter, scanPolicy)

	recordReader := aerospike.NewRecordReader(
		ctx, rc.aerospikeClient, readerConfig, rc.logger, aerospike.NewRecordsetCloser())
	defer recordReader.Close()

	count, err := countRecords(ctx, recordReader)
	if err != nil {
		return 0, err
	}

	return count * uint64(sumPartition(filters)), nil
}

// countRecords counts the records returned by the given reader.
func countRecords(ctx context.Context, recordReader aerospike.RecordReader) (uint64, error) {
	var count uint64

	for {
		if _, err := recordReader.Read(ctx); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return 0, fmt.Errorf("error during records counting: %w", err)
		}

		count++
	}

	return count, nil
}

// randomPartition returns a random partition from the given list of filters.
// #nosec G404
func randomPartition(partitionFilters []*a.PartitionFilter) *a.PartitionFilter {
	if len(partitionFilters) == 0 { // no filter => return any random partition.
		return a.NewPartitionFilterById(rand.Intn(MaxPartitions))
	}

	index := rand.Intn(len(partitionFilters))
	// get a random filter from the provided list of partition filters
	randomFilter := partitionFilters[index]
	// get random partition offset for the filter
	offset := rand.Intn(randomFilter.Count)

	return a.NewPartitionFilterById(randomFilter.Begin + offset)
}

// sumPartition returns total number of partitions in filters list.
func sumPartition(partitionFilters []*a.PartitionFilter) int {
	if len(partitionFilters) == 0 { // no filter => scan all partitions.
		return MaxPartitions
	}

	var totalPartitionCount int

	for _, pf := range partitionFilters {
		if pf.Count != 0 {
			totalPartitionCount += pf.Count
		} else {
			totalPartitionCount += len(pf.Partitions)
		}
	}

	return totalPartitionCount
}
