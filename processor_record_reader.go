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
	"github.com/aerospike/backup-go/pkg/asinfo"
	"golang.org/x/sync/semaphore"
)

type infoGetter interface {
	GetRecordCount(namespace string, sets []string) (uint64, error)
	GetRackNodes(rackID int) ([]string, error)
	GetService(node string) (string, error)
}

// recordReaderProcessorXDR configures and creates record readers pipelines.
type recordReaderProcessor[T models.TokenConstraint] struct {
	config *ConfigBackup
	// add scanConfig in the future.
	aerospikeClient AerospikeClient
	infoClient      *asinfo.Client
	state           *State
	scanLimiter     *semaphore.Weighted
	rpsCollector    *metrics.Collector

	logger *slog.Logger
}

// newRecordReaderProcessorXDR returns a new record reader processor.
func newRecordReaderProcessor[T models.TokenConstraint](
	config *ConfigBackup,
	aerospikeClient AerospikeClient,
	infoClient *asinfo.Client,
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

	// If we are paralleling scans by nodes.
	if rr.config.isParalleledByNodes() {
		return rr.newAerospikeReadWorkersForNodes(ctx, n, &scanPolicy)
	}

	return rr.newAerospikeReadWorkersForPartition(ctx, n, &scanPolicy)
}

func (rr *recordReaderProcessor[T]) newAerospikeReadWorkersForPartition(
	ctx context.Context, n int, scanPolicy *a.ScanPolicy,
) ([]pipe.Reader[*models.Token], error) {
	var err error

	partitionGroups := rr.config.PartitionFilters

	if !rr.config.isStateContinue() {
		partitionGroups, err = splitPartitions(rr.config.PartitionFilters, n)
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

	// If we have multiply partition filters, we shrink workers to number of filters.
	readers := make([]pipe.Reader[*models.Token], len(partitionGroups))

	for i := range partitionGroups {
		recordReaderConfig := rr.recordReaderConfigForPartitions(partitionGroups[i], scanPolicy)

		recordReader := aerospike.NewRecordReader(
			ctx,
			rr.aerospikeClient,
			recordReaderConfig,
			rr.logger,
		)

		readers[i] = recordReader
	}

	return readers, nil
}

func (rr *recordReaderProcessor[T]) newAerospikeReadWorkersForNodes(
	ctx context.Context, n int, scanPolicy *a.ScanPolicy,
) ([]pipe.Reader[*models.Token], error) {
	nodes, err := rr.getNodes()
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

	readers := make([]pipe.Reader[*models.Token], n)

	for i := 0; i < n; i++ {
		// Skip empty groups.
		if len(nodesGroups[i]) == 0 {
			continue
		}

		recordReaderConfig := rr.recordReaderConfigForNode(nodesGroups[i], scanPolicy)

		recordReader := aerospike.NewRecordReader(
			ctx,
			rr.aerospikeClient,
			recordReaderConfig,
			rr.logger,
		)

		readers[i] = recordReader
	}

	return readers, nil
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

func (rr *recordReaderProcessor[T]) recordReaderConfigForPartitions(
	partitionFilter *a.PartitionFilter,
	scanPolicy *a.ScanPolicy,
) *aerospike.RecordReaderConfig {
	pfCopy := *partitionFilter

	return aerospike.NewRecordReaderConfig(
		rr.config.Namespace,
		rr.config.SetList,
		&pfCopy,
		nil,
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

func (rr *recordReaderProcessor[T]) recordReaderConfigForNode(
	nodes []*a.Node,
	scanPolicy *a.ScanPolicy,
) *aerospike.RecordReaderConfig {
	return aerospike.NewRecordReaderConfig(
		rr.config.Namespace,
		rr.config.SetList,
		nil,
		nodes,
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
