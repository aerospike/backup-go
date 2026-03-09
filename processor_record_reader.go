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

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/internal/metrics"
	"github.com/aerospike/backup-go/io/aerospike"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe"
	"golang.org/x/sync/semaphore"
)

// recordReaderProcessorXDR configures and creates record readers pipelines.
type recordReaderProcessor[T models.TokenConstraint] struct {
	config *ConfigBackup
	// add scanConfig in the future.
	aerospikeClient AerospikeClient
	infoClient      InfoGetter
	state           *State
	scanLimiter     *semaphore.Weighted
	rpsCollector    *metrics.Collector
	throttler       *aerospike.ThrottleLimiter

	logger *slog.Logger
}

// newRecordReaderProcessorXDR returns a new record reader processor.
func newRecordReaderProcessor[T models.TokenConstraint](
	config *ConfigBackup,
	aerospikeClient AerospikeClient,
	infoClient InfoGetter,
	state *State,
	scanLimiter *semaphore.Weighted,
	rpsCollector *metrics.Collector,
	logger *slog.Logger,
	throttler *aerospike.ThrottleLimiter,
) *recordReaderProcessor[T] {
	logger.Debug("created new records reader processor")

	return &recordReaderProcessor[T]{
		config:          config,
		aerospikeClient: aerospikeClient,
		infoClient:      infoClient,
		scanLimiter:     scanLimiter,
		state:           state,
		rpsCollector:    rpsCollector,
		logger:          logger,
		throttler:       throttler,
	}
}

func (rr *recordReaderProcessor[T]) newAerospikeReadWorkers(ctx context.Context) ([]pipe.Reader[*models.Token], error) {
	scanPolicy := *rr.config.ScanPolicy

	// we need to set the RawCDT flag
	// in the scan policy so that maps and lists are returned as raw blob bins
	scanPolicy.RawCDT = true

	var (
		partitionGroups []*a.PartitionFilter
		err             error
	)

	// If the records are processed by nodes, we create the partition groups from
	// the primary partitions for the configured nodes.
	// Otherwise, we create the partition groups from the partition filters.
	if rr.config.isProcessedByNodes() {
		partitionGroups, err = rr.newPartitionGroupsFromNodes(ctx)
	} else {
		partitionGroups, err = rr.newPartitionGroups()
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create partition groups: %w", err)
	}

	// If we have multiply partition filters, we shrink workers to number of filters.
	readers := make([]pipe.Reader[*models.Token], len(partitionGroups))

	// Create record readers for each partition group.
	for i := range partitionGroups {
		recordReaderConfig := rr.newRecordReaderConfig(partitionGroups[i], &scanPolicy)

		recordReader := aerospike.NewRecordReader(
			ctx,
			rr.aerospikeClient,
			recordReaderConfig,
			rr.logger,
			aerospike.NewRecordsetCloser(),
		)

		readers[i] = recordReader
	}

	return readers, nil
}

// newPartitionGroups creates the partition groups from the partition filters.
func (rr *recordReaderProcessor[T]) newPartitionGroups() ([]*a.PartitionFilter, error) {
	var err error

	partitionGroups := rr.config.PartitionFilters

	if !rr.config.isStateContinue() {
		partitionGroups, err = splitPartitions(rr.config.PartitionFilters, rr.config.ParallelRead)
		if err != nil {
			return nil, err
		}

		if rr.config.isStateFirstRun() {
			// Init state.
			if err := rr.state.initState(partitionGroups); err != nil {
				return nil, err
			}
		}
	}

	return partitionGroups, nil
}

// newPartitionGroupsFromNodes creates the partition groups from the primary partitions for the configured nodes.
func (rr *recordReaderProcessor[T]) newPartitionGroupsFromNodes(ctx context.Context) ([]*a.PartitionFilter, error) {
	partIDs, err := rr.getPrimaryPartitions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary partitions: %w", err)
	}

	partitionGroups, err := splitPartitionIDs(partIDs, rr.config.ParallelRead)
	if err != nil {
		return nil, fmt.Errorf("failed to split partition ids: %w", err)
	}

	return partitionGroups, nil
}

// getPrimaryPartitions gets the primary partitions for the configured nodes.
func (rr *recordReaderProcessor[T]) getPrimaryPartitions(ctx context.Context) ([]int, error) {
	nodes, err := rr.getNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes: %w", err)
	}

	var partIDs []int

	for _, node := range nodes {
		parts, err := rr.infoClient.GetPrimaryPartitions(ctx, node.GetName(), rr.config.Namespace)
		if err != nil {
			return nil, fmt.Errorf("failed to get primary partitions for node: %s: %w", node.GetName(), err)
		}

		rr.logger.Debug("got partitions for node",
			slog.Any("partitions", parts),
			slog.String("node", node.GetName()))

		partIDs = append(partIDs, parts...)
	}

	return partIDs, nil
}

// getNodes gets active nodes from the cluster. The nodes are filtered by the node list and rack list
// if provided.
func (rr *recordReaderProcessor[T]) getNodes(ctx context.Context) ([]*a.Node, error) {
	nodesToFilter := rr.config.NodeList

	if len(rr.config.RackList) > 0 {
		nodeList := make([]string, 0)

		for _, rack := range rr.config.RackList {
			nodes, err := rr.infoClient.GetRackNodes(ctx, rack)
			if err != nil {
				return nil, fmt.Errorf("failed to get rack nodes: %w", err)
			}

			nodeList = append(nodeList, nodes...)
		}

		nodesToFilter = nodeList
	}

	nodes := rr.aerospikeClient.GetNodes()

	rr.logger.Debug("got nodes from cluster", slog.Any("nodes", nodes))

	// If bh.config.NodeList is not empty we filter nodes.
	nodes, err := rr.filterNodes(ctx, nodesToFilter, nodes)
	if err != nil {
		return nil, fmt.Errorf("failed to filter nodes: %w", err)
	}

	return nodes, nil
}

// filterNodes iterates over the nodes and selects only those nodes that are in nodesList.
// Returns a slice of filtered *a.Node and error.
func (rr *recordReaderProcessor[T]) filterNodes(ctx context.Context, nodesList []string, nodes []*a.Node,
) ([]*a.Node, error) {
	if len(nodesList) == 0 {
		return nodes, nil
	}

	nodesMap := make(map[string]struct{}, len(nodesList))
	for j := range nodesList {
		nodesMap[nodesList[j]] = struct{}{}
	}

	filteredNodes := make([]*a.Node, 0, len(nodesList))

	for _, node := range nodes {
		if !node.IsActive() {
			continue
		}

		nodeServiceAddress, err := rr.infoClient.GetService(ctx, node.GetName())
		if err != nil {
			return nil, fmt.Errorf("failed to get node %s service: %w", node.GetName(), err)
		}

		rr.logger.Debug("got service for node",
			slog.String("node", node.GetName()),
			slog.String("host", node.GetHost().String()),
			slog.String("service", nodeServiceAddress),
		)

		_, ok := nodesMap[nodeServiceAddress]
		if ok {
			filteredNodes = append(filteredNodes, node)
			continue
		}

		// If nodeList contains node names instead of address.
		_, ok = nodesMap[node.GetName()]
		if ok {
			filteredNodes = append(filteredNodes, node)
		}
	}

	// Check that we found all nodes.
	if len(filteredNodes) != len(nodesList) {
		return nil, fmt.Errorf("failed to find all nodes %d/%d in list: %v",
			len(filteredNodes), len(nodesList), nodesList)
	}

	return filteredNodes, nil
}

// newRecordReaderConfig creates a new record reader config for the given partition filter and scan policy.
func (rr *recordReaderProcessor[T]) newRecordReaderConfig(
	partitionFilter *a.PartitionFilter,
	scanPolicy *a.ScanPolicy,
) *aerospike.RecordReaderConfig {
	return aerospike.NewRecordReaderConfig(
		rr.config.Namespace,
		rr.config.SetList,
		partitionFilter,
		scanPolicy,
		rr.config.BinList,
		models.TimeBounds{
			FromTime: rr.config.ModAfter,
			ToTime:   rr.config.ModBefore,
		},
		rr.scanLimiter,
		rr.config.NoTTLOnly,
		rr.config.PageSize,
		rr.rpsCollector,
		rr.throttler,
	)
}
