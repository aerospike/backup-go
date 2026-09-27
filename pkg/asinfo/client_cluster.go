// Copyright 2024-2026 Aerospike, Inc.
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

package asinfo

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/aerospike/backup-go/errclass"
	"github.com/aerospike/backup-go/models"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

// GetVersion returns the lowest node version from the cluster.
func (ic *Client) GetVersion(ctx context.Context) (infomodels.AerospikeVersion, error) {
	return retryValue(ctx, ic.retryPolicy, func() (infomodels.AerospikeVersion, error) {
		var zero infomodels.AerospikeVersion

		nodes := ic.cluster.GetNodes()
		if len(nodes) == 0 {
			return zero, errNoNodesAvailable
		}

		var lowestVersion infomodels.AerospikeVersion

		for i, node := range nodes {
			currentVersion, err := ic.getAerospikeVersion(node)
			if err != nil {
				return zero, fmt.Errorf("failed to get version from node %s: %w", node.String(), err)
			}

			if i == 0 || lowestVersion.IsGreater(currentVersion) {
				lowestVersion = currentVersion
			}
		}

		return lowestVersion, nil
	})
}

// GetSIndexInfo returns information about secondary indexes in the given namespace.
func (ic *Client) GetSIndexInfo(ctx context.Context, namespace string) (models.SIndexInfo, error) {
	list, err := ic.getSIndexes(ctx, namespace, true)
	if err != nil {
		return models.SIndexInfo{}, err
	}

	var hasSetSIndex, hasExpressionSIndex bool

	for _, idx := range list {
		if idx.Expression != "" {
			hasExpressionSIndex = true
		}

		if idx.IndexType == models.SetSIndex {
			hasSetSIndex = true
		}
	}

	return models.SIndexInfo{
		HasSet:        hasSetSIndex,
		HasExpression: hasExpressionSIndex,
	}, nil
}

// GetSIndexes returns list of SIndexes for the given namespace.
func (ic *Client) GetSIndexes(ctx context.Context, namespace string) ([]*models.SIndex, error) {
	return ic.getSIndexes(ctx, namespace, false)
}

func (ic *Client) getSIndexes(ctx context.Context, namespace string, noWarn bool) ([]*models.SIndex, error) {
	return retryValue(ctx, ic.retryPolicy, func() ([]*models.SIndex, error) {
		node, aErr := ic.cluster.GetRandomNode()
		if aErr != nil {
			return nil, fmt.Errorf("%w: %w", errclass.ErrAerospike, aErr)
		}

		return ic.requestSIndexes(node, namespace, noWarn)
	})
}

// GetUDFs returns list of UDFs.
func (ic *Client) GetUDFs(ctx context.Context) ([]*models.UDF, error) {
	return retryValue(ctx, ic.retryPolicy, func() ([]*models.UDF, error) {
		node, aErr := ic.cluster.GetRandomNode()
		if aErr != nil {
			return nil, fmt.Errorf("%w: %w", errclass.ErrAerospike, aErr)
		}

		return ic.getUDFs(node)
	})
}

// SupportsBatchWrite reports whether the cluster version supports batch writes.
func (ic *Client) SupportsBatchWrite(ctx context.Context) (bool, error) {
	v, err := ic.GetVersion(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get aerospike version: %w", err)
	}

	return v.IsGreaterOrEqual(infomodels.AerospikeVersionSupportsBatchWrites), nil
}

// GetRecordCount counts number of records in given namespace and sets.
func (ic *Client) GetRecordCount(ctx context.Context, namespace string, sets []string) (uint64, error) {
	return retryValue(ctx, ic.retryPolicy, func() (uint64, error) {
		node, aErr := ic.cluster.GetRandomNode()
		if aErr != nil {
			return 0, fmt.Errorf("%w: %w", errclass.ErrAerospike, aErr)
		}

		effectiveReplicationFactor, err := ic.getEffectiveReplicationFactor(node, namespace)
		if err != nil {
			return 0, err
		}

		// If a database not started yet, it can respond with 0.
		if effectiveReplicationFactor == 0 {
			return 0, ErrReplicationFactorZero
		}

		var recordsNumber uint64

		for _, node := range ic.cluster.GetNodes() {
			if !node.IsActive() {
				continue
			}

			var recordCountForNode uint64

			switch {
			case len(sets) == 0:
				recordCountForNode, err = ic.getRecordCountForNodeNamespace(node, namespace)
			default:
				recordCountForNode, err = ic.getRecordCountForNode(node, namespace, sets)
			}

			if err != nil {
				return 0, err
			}

			recordsNumber += recordCountForNode
		}

		return recordsNumber / uint64(effectiveReplicationFactor), nil
	})
}

// GetPendingMigrations returns the number of pending migrations.
func (ic *Client) GetPendingMigrations(ctx context.Context, namespace string) (uint64, error) {
	return retryValue(ctx, ic.retryPolicy, func() (uint64, error) {
		result, err := ic.getClusterTotalMigrations(namespace)
		if err != nil {
			return 0, fmt.Errorf("failed to fetch migration stats: %w", err)
		}

		return result, nil
	})
}

// getClusterTotalMigrations sums up migrations from ALL nodes at once.
func (ic *Client) getClusterTotalMigrations(namespace string) (uint64, error) {
	nodes := ic.cluster.GetNodes()
	if len(nodes) == 0 {
		return 0, errNoNodesConnected
	}

	var total uint64

	for _, node := range nodes {
		migrations, err := ic.getPendingMigrations(node, namespace)
		if err != nil {
			return 0, err
		}

		total += migrations
	}

	return total, nil
}

func (ic *Client) getPendingMigrations(node infoGetter, namespace string) (uint64, error) {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDNamespaceInfo], namespace)

	response, aErr := ic.requestByNode(node, cmd)
	if aErr != nil {
		return 0, fmt.Errorf("%w: failed to get request info: %w", errclass.ErrAerospike, aErr)
	}

	resultMap, err := parseStdInfoResponse(response)
	if err != nil {
		return 0, fmt.Errorf("failed to parse record info request: %w", err)
	}

	var totalRemaining uint64

	for i := range resultMap {
		result, ok, err := resultMap[i].ParseUint64("migrate_tx_partitions_remaining")
		if err != nil {
			return 0, err
		}

		if ok {
			totalRemaining += result
		}

		result, ok, err = resultMap[i].ParseUint64("migrate_rx_partitions_remaining")
		if err != nil {
			return 0, err
		}

		if ok {
			totalRemaining += result
		}
	}

	return totalRemaining, nil
}

// GetNodesNames return list of active nodes names.
func (ic *Client) GetNodesNames() []string {
	nodes := ic.cluster.GetNodes()
	result := make([]string, 0, len(nodes))

	for _, node := range nodes {
		if node.IsActive() {
			result = append(result, node.GetName())
		}
	}

	return result
}

// GetSetsList returns the list of set names for the given namespace, excluding the MRT monitor set.
func (ic *Client) GetSetsList(ctx context.Context, namespace string) ([]string, error) {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDSetsOfNamespace], namespace)

	result, err := ic.requestRandomNode(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get sets: %w", err)
	}

	resultMap, err := parseStdInfoResponse(result)
	if err != nil {
		return nil, fmt.Errorf("failed to parse sets info: %w", err)
	}

	sets := make([]string, 0, len(resultMap))

	for _, rec := range resultMap {
		val, ok := rec["set"]
		if !ok {
			continue
		}

		if val == models.MonitorRecordsSetName {
			continue
		}

		sets = append(sets, val)
	}

	return sets, nil
}

// GetRackNodes returns list of nodes by rack id.
func (ic *Client) GetRackNodes(ctx context.Context, rackID int) ([]string, error) {
	cmd := ic.cmdDict[cmdIDRack]

	result, err := ic.requestRandomNode(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get racks info: %w", err)
	}

	resultMap, err := parseStdInfoResponse(result)
	if err != nil {
		return nil, fmt.Errorf("failed to parse racks info: %w", err)
	}

	var nodes []string

	rackKey := fmt.Sprintf("rack_%d", rackID)

	for _, v := range resultMap {
		for n, m := range v {
			if strings.EqualFold(rackKey, n) {
				nodes = strings.Split(m, ",")
			}
		}
	}

	if len(nodes) == 0 {
		return nil, fmt.Errorf("failed to find nodes for rack %d: %w", rackID, ErrNoNode)
	}

	return nodes, nil
}

// GetService returns service name by node name.
func (ic *Client) GetService(ctx context.Context, node string) (string, error) {
	// First request TLS name.
	result, err := retryValue(ctx, ic.retryPolicy, func() (string, error) {
		return ic.requestByNodeName(node, ic.cmdDict[cmdIDServiceTLSStd])
	})

	// If result is empty, then request plain.
	if result == "" {
		result, err = retryValue(ctx, ic.retryPolicy, func() (string, error) {
			return ic.requestByNodeName(node, ic.cmdDict[cmdIDServiceClearStd])
		})
	}

	return result, err
}

// GetNamespacesList returns list of namespaces.
func (ic *Client) GetNamespacesList(ctx context.Context) ([]string, error) {
	cmd := ic.cmdDict[cmdIDNamespaces]

	result, err := ic.requestRandomNode(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get namespaces list: %w", err)
	}

	return strings.Split(result, infoObjSep), nil
}

// GetStatus returns cluster status.
func (ic *Client) GetStatus(ctx context.Context) (string, error) {
	cmd := ic.cmdDict[cmdIDStatus]

	result, err := ic.requestRandomNode(ctx, cmd)
	if err != nil {
		return "", fmt.Errorf("failed to get status info: %w", err)
	}

	return result, nil
}

// GetPrimaryPartitions returns a list of primary partitions.
func (ic *Client) GetPrimaryPartitions(ctx context.Context, node, namespace string) ([]int, error) {
	return retryValue(ctx, ic.retryPolicy, func() ([]int, error) {
		return ic.getPrimaryPartitions(node, namespace)
	})
}

func (ic *Client) getPrimaryPartitions(node, namespace string) ([]int, error) {
	cmd := ic.cmdDict[cmdIDReplicas]

	result, err := ic.requestByNodeName(node, cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get by node command: %s: %w", redactCmd(cmd), err)
	}

	var base64Res string
	// Looks like the "replicas" response didn't look like any known info responses,
	// so we can't use standard parsing func.
	for nsRes := range strings.SplitSeq(result, ";") {
		res := strings.Split(nsRes, ":")
		if len(res) != 2 {
			// Skip potentially broken response.
			continue
		}

		if res[0] == namespace {
			data := strings.Split(res[1], ",")
			// Guard against malformed responses to avoid an index-out-of-range panic.
			if len(data) < minReplicasFields {
				continue
			}

			base64Res = data[2]
		}
	}

	if base64Res == "" {
		return nil, fmt.Errorf("%w: failed to find replicas for node %s", errclass.ErrNotFound, node)
	}

	bitMap, err := base64StringToBitArray(base64Res)
	if err != nil {
		return nil, fmt.Errorf("failed to parse primary partition bitmap: %w", err)
	}

	return bitMapToIntSlice(bitMap), nil
}

func (ic *Client) getUDFs(node infoGetter) ([]*models.UDF, error) {
	cmd := ic.cmdDict[cmdIDUdfList]

	cmdResp, err := ic.requestByNode(node, cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to request udf-list: %w", err)
	}

	udfList, err := parseUDFListResponse(cmdResp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse udf-list info response: %w", err)
	}

	// No UDFs
	if udfList == nil {
		return nil, nil
	}

	udfs := make([]*models.UDF, len(udfList))

	for i, udfMap := range udfList {
		name, ok := udfMap["filename"]
		if !ok {
			return nil, errUDFMissingFilename
		}

		udf, err := ic.getUDF(node, name)
		if err != nil {
			return nil, fmt.Errorf("failed to get UDF: %w", err)
		}

		udfs[i] = udf
	}

	return udfs, nil
}

func (ic *Client) getUDF(node infoGetter, name string) (*models.UDF, error) {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDUdfGetFilename], name)

	cmdResp, err := ic.requestByNode(node, cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to request UDF: %w", err)
	}

	udf, err := parseUDFResponse(cmdResp)
	if err != nil {
		return nil, err
	}

	udf.Name = name

	return udf, nil
}

func (ic *Client) getRecordCountForNode(node infoGetter, namespace string, sets []string,
) (uint64, error) {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDSetsOfNamespace], namespace)

	response, aErr := ic.requestByNode(node, cmd)
	if aErr != nil {
		return 0, fmt.Errorf("%w: failed to get record count: %w", errclass.ErrAerospike, aErr)
	}

	infoResponse, err := parseStdInfoResponse(response)
	if err != nil {
		return 0, fmt.Errorf("failed to parse record info request: %w", err)
	}

	var recordsNumber uint64

	for _, setInfo := range infoResponse {
		setName, ok := setInfo["set"]
		if !ok {
			return 0, fmt.Errorf("%w: set name missing in response %s", errclass.ErrAerospike, response)
		}

		// Skip MRT monitor records.
		if setName == models.MonitorRecordsSetName {
			continue
		}

		if len(sets) == 0 || slices.Contains(sets, setName) {
			objectCount, ok := setInfo["objects"]
			if !ok {
				return 0, fmt.Errorf("%w: objects number missing in response %s", errclass.ErrAerospike, response)
			}

			objects, err := strconv.ParseUint(objectCount, 10, 64)
			if err != nil {
				return 0, err
			}

			recordsNumber += objects
		}
	}

	return recordsNumber, nil
}

func (ic *Client) getRecordCountForNodeNamespace(node infoGetter, namespace string,
) (uint64, error) {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDNamespaceInfo], namespace)

	response, aErr := ic.requestByNode(node, cmd)
	if aErr != nil {
		return 0, fmt.Errorf("%w: failed to request info: %w", errclass.ErrAerospike, aErr)
	}

	resultMap, err := parseStdInfoResponse(response)
	if err != nil {
		return 0, fmt.Errorf("failed to parse record info request: %w", err)
	}

	for i := range resultMap {
		result, ok, err := resultMap[i].ParseUint64("objects")
		if err != nil {
			return 0, err
		}

		if ok {
			return result, nil
		}
	}

	return 0, errParseRecordInfo
}

func (ic *Client) getEffectiveReplicationFactor(node infoGetter, namespace string,
) (int, error) {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDNamespaceInfo], namespace)

	response, aErr := ic.requestByNode(node, cmd)
	if aErr != nil {
		return 0, fmt.Errorf("%w: failed to get namespace info: %w", errclass.ErrAerospike, aErr)
	}

	infoResponse, err := parseStdInfoResponse(response)
	if err != nil {
		return 0, fmt.Errorf("failed to parse record info request: %w", err)
	}

	for _, r := range infoResponse {
		factor, ok := r["effective_replication_factor"]
		if ok {
			return strconv.Atoi(factor)
		}
	}

	return 0, errReplicationFactorNotFound
}

// GetClusterStable checks the stability of a cluster within the specified namespace and retries on transient errors.
// Returns a boolean indicating the stability status and an error if the operation fails after retries.
func (ic *Client) GetClusterStable(ctx context.Context, namespace string) (bool, error) {
	return retryValue(ctx, ic.retryPolicy, func() (bool, error) {
		return ic.getClusterStable(namespace)
	})
}

func (ic *Client) getClusterStable(namespace string) (bool, error) {
	nodes := ic.cluster.GetNodes()
	nodesNum := len(nodes)

	stats, err := ic.getStatistics()
	if err != nil {
		return false, fmt.Errorf("failed to get cluster statistics: %w", err)
	}

	clusterKey, ok := searchInInfoResponse(stats, "cluster_key")
	if !ok {
		return false, fmt.Errorf("%w: cluster key not found in statistics", errclass.ErrAerospike)
	}

	for _, node := range nodes {
		cmd := fmt.Sprintf(ic.cmdDict[cmdIDClusterStable], nodesNum, namespace)

		result, err := ic.requestRandomNodeOnce(cmd)
		if err != nil {
			return false, fmt.Errorf("failed to get node %s stable status: %w", node.GetName(), err)
		}

		if result != clusterKey {
			return false, fmt.Errorf("%w: cluster %s is not stable, result is %s", errclass.ErrAerospike, clusterKey, result)
		}
	}

	return true, nil
}

// getStatistics returns cluster statistics. Every caller runs inside a retried
// operation, so a single info request is made here.
func (ic *Client) getStatistics() ([]infomodels.InfoMap, error) {
	cmd := ic.cmdDict[cmdIDStatistics]

	result, err := ic.requestRandomNodeOnce(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster statistics: %w", err)
	}

	infoResponse, err := parseStatisticsResponse(result)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cluster statistics info response: %w", err)
	}

	return infoResponse, nil
}
