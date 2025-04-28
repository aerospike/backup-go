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
	"math/rand"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/internal/asinfo"
	"github.com/aerospike/backup-go/io/aerospike"
)

func (bh *backupRecordsHandler) countRecords(ctx context.Context, infoClient *asinfo.InfoClient) (uint64, error) {
	if bh.config.isFullBackup() {
		return bh.countUsingInfoClient(infoClient)
	}

	return bh.countRecordsUsingScan(ctx)
}

func (bh *backupRecordsHandler) countUsingInfoClient(infoClient *asinfo.InfoClient) (uint64, error) {
	totalRecordCount, err := infoClient.GetRecordCount(bh.config.Namespace, bh.config.SetList)
	if err != nil {
		return 0, fmt.Errorf("failed to get record count: %w", err)
	}

	partitionsToScan := uint64(sumPartition(bh.config.PartitionFilters))
	return totalRecordCount * MaxPartitions / partitionsToScan, nil
}

func (bh *backupRecordsHandler) countRecordsUsingScan(ctx context.Context) (uint64, error) {
	scanPolicy := *bh.config.ScanPolicy

	scanPolicy.IncludeBinData = false
	scanPolicy.MaxRecords = 0

	if bh.config.isParalleledByNodes() {
		return bh.countRecordsUsingScanByNodes(ctx, &scanPolicy)
	}

	return bh.countRecordsUsingScanByPartitions(ctx, &scanPolicy)
}

func (bh *backupRecordsHandler) countRecordsUsingScanByPartitions(ctx context.Context, scanPolicy *a.ScanPolicy,
) (uint64, error) {
	var count uint64

	partitionFilter := randomPartition(bh.config.PartitionFilters)

	readerConfig := bh.recordReaderConfigForPartitions(partitionFilter, scanPolicy)
	recordReader := aerospike.NewRecordReader(ctx, bh.aerospikeClient, readerConfig, bh.logger)
	defer recordReader.Close()

	for {
		if _, err := recordReader.Read(); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return 0, fmt.Errorf("error during records counting: %w", err)
		}

		count++
	}

	return count * uint64(sumPartition(bh.config.PartitionFilters)), nil
}

func (bh *backupRecordsHandler) countRecordsUsingScanByNodes(ctx context.Context, scanPolicy *a.ScanPolicy,
) (uint64, error) {
	nodes, err := bh.getNodes()
	if err != nil {
		return 0, fmt.Errorf("failed to get nodes: %w", err)
	}

	randomIndex := rand.Intn(len(nodes))
	randomNode := []*a.Node{nodes[randomIndex]}
	readerConfig := bh.recordReaderConfigForNode(randomNode, scanPolicy)
	recordReader := aerospike.NewRecordReader(ctx, bh.aerospikeClient, readerConfig, bh.logger)
	defer recordReader.Close()

	var count uint64
	for {
		if _, err := recordReader.Read(); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return 0, fmt.Errorf("error during records counting: %w", err)
		}

		count++
	}

	return count * uint64(len(nodes)), nil
}

// randomPartition returns random partition from filters list.
func randomPartition(partitionFilters []*a.PartitionFilter) *a.PartitionFilter {
	if len(partitionFilters) == 0 { // no filter => return any random partition.
		return a.NewPartitionFilterById(rand.Intn(MaxPartitions))
	}

	allPartitionIDs := make([]int, 0, sumPartition(partitionFilters))

	for _, pf := range partitionFilters {
		for i := 0; i < pf.Count; i++ {
			partitionID := pf.Begin + i
			allPartitionIDs = append(allPartitionIDs, partitionID)
		}
	}

	randomIndex := rand.Intn(len(allPartitionIDs))
	randomPartitionID := allPartitionIDs[randomIndex]

	return a.NewPartitionFilterById(randomPartitionID)
}

// sumPartition returns total number of partitions in filters list.
func sumPartition(partitionFilters []*a.PartitionFilter) int {
	if len(partitionFilters) == 0 { // no filter => scan all partitions.
		return MaxPartitions
	}

	var totalPartitionCount int

	for _, pf := range partitionFilters {
		totalPartitionCount += pf.Count
	}

	return totalPartitionCount
}
