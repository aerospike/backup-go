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
	}
}

func (rr *recordReaderProcessor[T]) newAerospikeReadWorkers(
	ctx context.Context, n int,
) ([]pipe.Reader[*models.Token], error) {
	scanPolicy := *rr.config.ScanPolicy

	// we need to set the RawCDT flag
	// in the scan policy so that maps and lists are returned as raw blob bins
	scanPolicy.RawCDT = true

	var (
		partitionGroups []*a.PartitionFilter
		err             error
	)

	if rr.config.isProcessedByNodes() {
		partitionGroups, err = rr.newPartitionGroupsFromNodes(n)
	} else {
		partitionGroups, err = rr.newPartitionGroups(n)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create partition groups: %w", err)
	}

	// If we have multiply partition filters, we shrink workers to number of filters.
	readers := make([]pipe.Reader[*models.Token], len(partitionGroups))

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

func (rr *recordReaderProcessor[T]) newPartitionGroups(numWorkers int) ([]*a.PartitionFilter, error) {
	var err error

	partitionGroups := rr.config.PartitionFilters

	if !rr.config.isStateContinue() {
		partitionGroups, err = splitPartitions(rr.config.PartitionFilters, numWorkers)
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

func (rr *recordReaderProcessor[T]) newPartitionGroupsFromNodes(numWorkers int) ([]*a.PartitionFilter, error) {
	nodes, err := rr.getNodes()
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes: %w", err)
	}

	var partIDs []int

	for _, node := range nodes {
		parts, err := rr.infoClient.GetPrimaryPartitions(node.GetName(), rr.config.Namespace)
		if err != nil {
			return nil, fmt.Errorf("failed to get primary partitions for node: %s: %w", node.GetName(), err)
		}

		rr.logger.Debug("got partitions for node",
			slog.Any("partitions", parts),
			slog.String("node", node.GetName()))

		partIDs = append(partIDs, parts...)
	}

	partitionGroups, err := splitPartitionIDs(partIDs, numWorkers)
	if err != nil {
		return nil, fmt.Errorf("failed to split partition ids: %w", err)
	}

	return partitionGroups, nil
}

func (rr *recordReaderProcessor[T]) getNodes() ([]*a.Node, error) {
	nodesToFilter := rr.config.NodeList

	if len(rr.config.RackList) > 0 {
		nodeList := make([]string, 0)

		for _, rack := range rr.config.RackList {
			nodes, err := rr.infoClient.GetRackNodes(rack)
			if err != nil {
				return nil, fmt.Errorf("failed to get rack nodes: %w", err)
			}

			nodeList = append(nodeList, nodes...)
		}

		nodesToFilter = nodeList
	}

	nodes := rr.aerospikeClient.GetNodes()

	rr.logger.Info("got nodes from cluster", slog.Any("nodes", nodes))

	// If bh.config.NodeList is not empty we filter nodes.
	nodes, err := rr.filterNodes(nodesToFilter, nodes)
	if err != nil {
		return nil, fmt.Errorf("failed to filter nodes: %w", err)
	}

	return nodes, nil
}

// filterNodes iterates over the nodes and selects only those nodes that are in nodesList.
// Returns a slice of filtered *a.Node and error.
func (rr *recordReaderProcessor[T]) filterNodes(nodesList []string, nodes []*a.Node) ([]*a.Node, error) {
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

		nodeServiceAddress, err := rr.infoClient.GetService(nodes[i].GetName())
		if err != nil {
			return nil, fmt.Errorf("failed to get node %s service: %w", nodes[i].GetName(), err)
		}

		rr.logger.Info("got service for node",
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
	)
}
