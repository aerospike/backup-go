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
	"fmt"
	"log/slog"
	"sync/atomic"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/internal/processors"
	"github.com/aerospike/backup-go/io/aerospike"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"golang.org/x/sync/semaphore"
)

type infoGetter interface {
	GetRecordCount(namespace string, sets []string) (uint64, error)
	GetRackNodes(rackID int) ([]string, error)
	GetService(node string) (string, error)
}

type backupRecordsHandler struct {
	config          *ConfigBackup
	aerospikeClient AerospikeClient
	infoClient      infoGetter
	logger          *slog.Logger
	scanLimiter     *semaphore.Weighted
	state           *State
	pl              *pipeline.Pipeline[*models.Token]
	// records per second collector.
	rpsCollector *metrics.Collector
	// kilobytes per second collector.
	kbpsCollector *metrics.Collector
}

func newBackupRecordsHandler(
	config *ConfigBackup,
	ac AerospikeClient,
	infoClient infoGetter,
	logger *slog.Logger,
	scanLimiter *semaphore.Weighted,
	state *State,
	rpsCollector *metrics.Collector,
	kbpsCollector *metrics.Collector,
) *backupRecordsHandler {
	logger.Debug("created new backup records handler")

	h := &backupRecordsHandler{
		config:          config,
		aerospikeClient: ac,
		infoClient:      infoClient,
		logger:          logger,
		scanLimiter:     scanLimiter,
		state:           state,
		rpsCollector:    rpsCollector,
		kbpsCollector:   kbpsCollector,
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

	// Calculate the Records Per Second (RPS) target for each individual parallel reader/processor.
	// This value evenly distributes the overall read rate across the parallel workers,
	// ensuring each worker adheres to a portion of the total RPS limit.
	rps := bh.config.RecordsPerSecond / bh.config.ParallelRead

	composeProcessor := newTokenWorker(processors.NewComposeProcessor(
		processors.NewRecordCounter[*models.Token](recordsReadTotal),
		processors.NewVoidTimeSetter[*models.Token](bh.logger),
		processors.NewTPSLimiter[*models.Token](
			ctx, rps),
	), bh.config.ParallelRead)

	pl, err := pipeline.NewPipeline(
		bh.config.PipelinesMode, nil,
		readWorkers,
		composeProcessor,
		writers)
	if err != nil {
		return fmt.Errorf("failed to create new pipeline: %w", err)
	}

	// Assign, so we can get pl stats.
	bh.pl = pl

	return pl.Run(ctx)
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

		if bh.config.isStateFirstRun() {
			// Init state.
			if err := bh.state.initState(partitionGroups); err != nil {
				return nil, err
			}
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
	nodes, err := bh.getNodes()
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes: %w", err)
	}

	// As we can have nodes < workers, we can't distribute a small number of nodes to a large number of workers.
	// So we set workers = nodes.
	if len(nodes) < n {
		n = len(nodes)
	}

	nodesGroups, err := splitNodes(nodes, n)
	if err != nil {
		return nil, fmt.Errorf("failed to split nodes: %w", err)
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

func (bh *backupRecordsHandler) getNodes() ([]*a.Node, error) {
	nodesToFilter := bh.config.NodeList

	if len(bh.config.RackList) > 0 {
		nodeList := make([]string, 0)

		for _, rack := range bh.config.RackList {
			nodes, err := bh.infoClient.GetRackNodes(rack)
			if err != nil {
				return nil, fmt.Errorf("failed to get rack nodes: %w", err)
			}

			nodeList = append(nodeList, nodes...)
		}

		nodesToFilter = nodeList
	}

	nodes := bh.aerospikeClient.GetNodes()

	bh.logger.Info("got nodes from cluster", slog.Any("nodes", nodes))

	// If bh.config.NodeList is not empty we filter nodes.
	nodes, err := bh.filterNodes(nodesToFilter, nodes)
	if err != nil {
		return nil, fmt.Errorf("failed to filter nodes: %w", err)
	}

	return nodes, nil
}

// filterNodes iterates over the nodes and selects only those nodes that are in nodesList.
// Returns a slice of filtered *a.Node and error.
func (bh *backupRecordsHandler) filterNodes(nodesList []string, nodes []*a.Node) ([]*a.Node, error) {
	if len(nodesList) == 0 {
		return nodes, nil
	}

	nodesMap := make(map[string]struct{}, len(nodesList))
	for j := range nodesList {
		nodesMap[nodesList[j]] = struct{}{}
	}

	filteredNodes := make([]*a.Node, 0, len(nodesList))

	for i := range nodes {
		if !nodes[i].IsActive() {
			continue
		}

		nodeServiceAddress, err := bh.infoClient.GetService(nodes[i].GetName())
		if err != nil {
			return nil, fmt.Errorf("failed to get node %s service: %w", nodes[i].GetName(), err)
		}

		bh.logger.Info("got service for node",
			slog.String("node", nodes[i].GetName()),
			slog.String("host", nodes[i].GetHost().String()),
			slog.String("service", nodeServiceAddress),
		)

		_, ok := nodesMap[nodeServiceAddress]
		if ok {
			filteredNodes = append(filteredNodes, nodes[i])
			continue
		}

		// If nodeList contains node names instead of address.
		_, ok = nodesMap[nodes[i].GetName()]
		if ok {
			filteredNodes = append(filteredNodes, nodes[i])
		}
	}

	// Check that we found all nodes.
	if len(filteredNodes) != len(nodesList) {
		return nil, fmt.Errorf("failed to find all nodes %d/%d in list: %v",
			len(filteredNodes), len(nodesList), nodesList)
	}

	return filteredNodes, nil
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
		bh.config.PageSize,
		bh.rpsCollector,
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
		bh.config.PageSize,
		bh.rpsCollector,
	)
}

// GetMetrics returns the rpsCollector of the backup job.
func (bh *backupRecordsHandler) GetMetrics() *models.Metrics {
	if bh == nil {
		return nil
	}

	var pr, pw int
	if bh.pl != nil {
		pr, pw = bh.pl.GetMetrics()
	}

	return models.NewMetrics(
		pr, pw,
		bh.rpsCollector.GetLastResult(),
		bh.kbpsCollector.GetLastResult(),
	)
}
