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
	"sync"
	"sync/atomic"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/internal/asinfo"
	"github.com/aerospike/backup-go/internal/processors"
	"github.com/aerospike/backup-go/io/aerospike"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"golang.org/x/sync/semaphore"
)

type backupRecordsHandler struct {
	config          *BackupConfig
	aerospikeClient AerospikeClient
	logger          *slog.Logger
	scanLimiter     *semaphore.Weighted
	state           *State
}

func newBackupRecordsHandler(
	config *BackupConfig,
	ac AerospikeClient,
	logger *slog.Logger,
	scanLimiter *semaphore.Weighted,
	state *State,
) *backupRecordsHandler {
	logger.Debug("created new backup records handler")

	h := &backupRecordsHandler{
		config:          config,
		aerospikeClient: ac,
		logger:          logger,
		scanLimiter:     scanLimiter,
		state:           state,
	}

	return h
}

func (bh *backupRecordsHandler) run(
	ctx context.Context,
	writers []pipeline.Worker[*models.Token],
	recordsReadTotal *atomic.Uint64,
) error {
	readWorkers, err := bh.makeAerospikeReadWorkers(ctx, bh.config.ParallelRead)
	if err != nil {
		return err
	}

	composeProcessor := newTokenWorker(processors.NewComposeProcessor(
		processors.NewRecordCounter(recordsReadTotal),
		processors.NewVoidTimeSetter(bh.logger),
		processors.NewTPSLimiter[*models.Token](
			ctx, bh.config.RecordsPerSecond),
	), bh.config.ParallelRead)

	return pipeline.NewPipeline(true, readWorkers, composeProcessor, writers).Run(ctx)
}

func (bh *backupRecordsHandler) countRecords(ctx context.Context, infoClient *asinfo.InfoClient) (uint64, error) {
	if bh.config.isFullBackup() {
		return infoClient.GetRecordCount(bh.config.Namespace, bh.config.SetList)
	}

	return bh.countRecordsUsingScan(ctx)
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
	var (
		count atomic.Uint64
		wg    sync.WaitGroup
	)

	errorsCh := make(chan error, len(bh.config.PartitionFilters))

	for i := range bh.config.PartitionFilters {
		wg.Add(1)

		j := i

		go func() {
			defer wg.Done()

			// We should copy *bh.config.PartitionFilters[i] value, to avoid getting zero results from other scans.
			// As after filter is applied for any scan it set .Done = true, after that no records will be returned
			// with this filter.
			pf := *bh.config.PartitionFilters[j]
			readerConfig := bh.recordReaderConfigForPartitions(&pf, scanPolicy)
			recordReader := aerospike.NewRecordReader(ctx, bh.aerospikeClient, readerConfig, bh.logger)

			for {
				if _, err := recordReader.Read(); err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					errorsCh <- fmt.Errorf("error during records counting: %w", err)
				}

				count.Add(1)
			}

			recordReader.Close()
		}()
		errorsCh <- nil
	}

	// Wait for all count goroutines to finish.
	go func() {
		wg.Wait()
		close(errorsCh)
	}()

	for err := range errorsCh {
		if err != nil {
			return 0, err
		}
	}

	return count.Load(), nil
}

func (bh *backupRecordsHandler) countRecordsUsingScanByNodes(ctx context.Context, scanPolicy *a.ScanPolicy,
) (uint64, error) {
	nodes := bh.aerospikeClient.GetNodes()
	nodes = filterNodes(bh.config.NodeList, nodes)

	var count uint64

	readerConfig := bh.recordReaderConfigForNode(nodes, scanPolicy)
	recordReader := aerospike.NewRecordReader(ctx, bh.aerospikeClient, readerConfig, bh.logger)

	for {
		if _, err := recordReader.Read(); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return 0, fmt.Errorf("error during records counting: %w", err)
		}

		count++
	}

	recordReader.Close()

	return count, nil
}

func (bh *backupRecordsHandler) makeAerospikeReadWorkers(
	ctx context.Context, n int,
) ([]pipeline.Worker[*models.Token], error) {
	scanPolicy := *bh.config.ScanPolicy

	// we need to set the RawCDT flag
	// in the scan policy so that maps and lists are returned as raw blob bins
	scanPolicy.RawCDT = true

	// If we are paralleling scans by nodes.
	if bh.config.isParalleledByNodes() {
		return bh.makeAerospikeReadWorkersForNodes(ctx, n, &scanPolicy)
	}

	return bh.makeAerospikeReadWorkersForPartition(ctx, n, &scanPolicy)
}

func (bh *backupRecordsHandler) makeAerospikeReadWorkersForPartition(
	ctx context.Context, n int, scanPolicy *a.ScanPolicy,
) ([]pipeline.Worker[*models.Token], error) {
	var err error

	partitionGroups := bh.config.PartitionFilters

	if !bh.config.isStateContinue() {
		partitionGroups, err = splitPartitions(bh.config.PartitionFilters, n)
		if err != nil {
			return nil, err
		}
	}

	// If we have multiply partition filters, we shrink workers to number of filters.
	readWorkers := make([]pipeline.Worker[*models.Token], len(partitionGroups))

	for i := range partitionGroups {
		recordReaderConfig := bh.recordReaderConfigForPartitions(partitionGroups[i], scanPolicy)

		recordReader := aerospike.NewRecordReader(
			ctx,
			bh.aerospikeClient,
			recordReaderConfig,
			bh.logger,
		)

		readWorkers[i] = pipeline.NewReadWorker[*models.Token](recordReader)
	}

	return readWorkers, nil
}

func (bh *backupRecordsHandler) makeAerospikeReadWorkersForNodes(
	ctx context.Context, n int, scanPolicy *a.ScanPolicy,
) ([]pipeline.Worker[*models.Token], error) {
	nodes := bh.aerospikeClient.GetNodes()
	// If bh.config.NodeList is not empty we filter nodes.
	nodes = filterNodes(bh.config.NodeList, nodes)
	// As we can have nodes < workers, we can't distribute a small number of nodes to a large number of workers.
	// So we set workers = nodes.
	if len(nodes) < n {
		n = len(nodes)
	}

	nodesGroups, err := splitNodes(nodes, n)
	if err != nil {
		return nil, err
	}

	readWorkers := make([]pipeline.Worker[*models.Token], n)

	for i := 0; i < n; i++ {
		// Skip empty groups.
		if len(nodesGroups[i]) == 0 {
			continue
		}

		recordReaderConfig := bh.recordReaderConfigForNode(nodesGroups[i], scanPolicy)

		recordReader := aerospike.NewRecordReader(
			ctx,
			bh.aerospikeClient,
			recordReaderConfig,
			bh.logger,
		)

		readWorkers[i] = pipeline.NewReadWorker[*models.Token](recordReader)
	}

	return readWorkers, nil
}

func (bh *backupRecordsHandler) recordReaderConfigForPartitions(
	partitionFilter *a.PartitionFilter,
	scanPolicy *a.ScanPolicy,
) *aerospike.RecordReaderConfig {
	pfCopy := *partitionFilter

	return aerospike.NewRecordReaderConfig(
		bh.config.Namespace,
		bh.config.SetList,
		&pfCopy,
		nil,
		scanPolicy,
		bh.config.BinList,
		models.TimeBounds{
			FromTime: bh.config.ModAfter,
			ToTime:   bh.config.ModBefore,
		},
		bh.scanLimiter,
		bh.config.NoTTLOnly,
		1000,
	)
}

func (bh *backupRecordsHandler) recordReaderConfigForNode(
	nodes []*a.Node,
	scanPolicy *a.ScanPolicy,
) *aerospike.RecordReaderConfig {
	return aerospike.NewRecordReaderConfig(
		bh.config.Namespace,
		bh.config.SetList,
		nil,
		nodes,
		scanPolicy,
		bh.config.BinList,
		models.TimeBounds{
			FromTime: bh.config.ModAfter,
			ToTime:   bh.config.ModBefore,
		},
		bh.scanLimiter,
		bh.config.NoTTLOnly,
		100,
	)
}
