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

package asinfo

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"

	a "github.com/aerospike/aerospike-client-go/v8"
	cltime "github.com/aerospike/backup-go/internal/citrusleaf_time"
	"github.com/aerospike/backup-go/models"
	iModels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

const errCmdRespPrefix = "ERROR"

const (
	indexTypeDefault   = "default"
	indexTypeNone      = "none"
	indexTypeList      = "list"
	indexTypeMapKeys   = "mapkeys"
	indexTypeMapValues = "mapvalues"

	indexBinTypeNumeric     = "numeric"
	indexBinTypeIntSigned   = "int signed"
	indexBinTypeString      = "string"
	indexBinTypeText        = "text"
	indexBinTypeBlob        = "blob"
	indexBinTypeGeo2DSphere = "geo2dsphere"
	indexBinTypeGeoJSON     = "geojson"

	jobTypeBackup = "backup"
)

const (
	// partitionsPerNamespace is the fixed number of partitions every Aerospike
	// namespace is split into. Used to normalize aggregated backup progress.
	partitionsPerNamespace = 4096
	// percentBase converts a percentage value (0-100) into a ratio.
	percentBase = 100
	// minReplicasFields is the minimum number of comma-separated fields expected
	// in a single namespace entry of the "replicas" info response.
	minReplicasFields = 3
)

var (
	ErrReplicationFactorZero = errors.New("replication factor is zero")
	ErrNoNode                = errors.New("no node found")
	ErrNotFound              = errors.New("not found")

	// Static internal errors. Kept as package-level sentinels so they can be
	// matched with errors.Is and satisfy err113/perfsprint linters.
	errNoInfoCommands            = errors.New("no info commands provided or command not supported")
	errNoNodesAvailable          = errors.New("no nodes available in cluster")
	errNoNodesConnected          = errors.New("no nodes connected")
	errReplicationFactorNotFound = errors.New("replication factor not found")
	errParseRecordInfo           = errors.New("failed to parse record info request")
	errUDFMissingFilename        = errors.New("udf-list response missing filename")

	secretAgentValRegex = regexp.MustCompile(`(.+?)=secrets:(.+?):(.+?)`)
)

// infoGetter defines the methods for doing info requests with the Aerospike database.
// Is used for tests.
type infoGetter interface {
	RequestInfo(infoPolicy *a.InfoPolicy, commands ...string) (map[string]string, a.Error)
}

// NodeGetter describes aerospike.Cluster object.
type NodeGetter interface {
	GetRandomNode() (*a.Node, a.Error)
	GetNodeByName(name string) (*a.Node, a.Error)
	GetNodes() []*a.Node
}

// Client manages asinfo interactions with an Aerospike cluster, handling policies, retry logic, and command operations.
type Client struct {
	cluster     NodeGetter
	policy      *a.InfoPolicy
	retryPolicy *models.RetryPolicy
	cmdDict     map[int]string
}

// NewClient initializes and returns a new asinfo Client instance with the provided Aerospike client,
// policy, and retry policy.
func NewClient(
	cluster NodeGetter,
	policy *a.InfoPolicy,
	retryPolicy *models.RetryPolicy,
) (*Client, error) {
	if retryPolicy == nil {
		retryPolicy = models.NewDefaultRetryPolicy()
	}

	ic := &Client{
		cluster:     cluster,
		policy:      policy,
		retryPolicy: retryPolicy,
	}
	// On init we can use context.Background(), as we don't need to do any async operations.
	ctx := context.Background()

	v, err := ic.GetVersion(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get aerospike version: %w", err)
	}

	ic.cmdDict = newCmdDict(v)

	return ic, nil
}

// GetInfo runs the given info commands against a random cluster node with retries.
func (ic *Client) GetInfo(ctx context.Context, names ...string) (map[string]string, error) {
	// Check if any info commands are provided or command is not supported.
	if len(names) == 0 || names[0] == "" {
		return nil, errNoInfoCommands
	}

	var result map[string]string

	err := executeWithRetry(ctx, ic.retryPolicy, func() error {
		node, err := ic.cluster.GetRandomNode()
		if err != nil {
			return err
		}

		result, err = node.RequestInfo(ic.policy, names...)

		return err
	})

	return result, err
}

func (ic *Client) requestByNode(nodeName string, names ...string) (map[string]string, error) {
	node, err := ic.cluster.GetNodeByName(nodeName)
	if err != nil {
		return nil, err
	}

	result, err := node.RequestInfo(ic.policy, names...)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// execByNode runs cmd on the named node and validates the result response.
// action is a short verb phrase used to build the error messages, e.g. "create xdr dc".
func (ic *Client) execByNode(nodeName, cmd, action string) error {
	resp, err := ic.requestByNode(nodeName, cmd)
	if err != nil {
		return fmt.Errorf("failed to %s: %w", action, err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse %s response: %w", action, err)
	}

	return nil
}

// GetVersion returns the lowest node version from the cluster.
func (ic *Client) GetVersion(ctx context.Context) (iModels.AerospikeVersion, error) {
	var result iModels.AerospikeVersion

	err := executeWithRetry(ctx, ic.retryPolicy, func() error {
		nodes := ic.cluster.GetNodes()
		if len(nodes) == 0 {
			return errNoNodesAvailable
		}

		var lowestVersion iModels.AerospikeVersion

		for i, node := range nodes {
			currentVersion, err := ic.getAerospikeVersion(node, ic.policy)
			if err != nil {
				return fmt.Errorf("failed to get version from node %s: %w", node.String(), err)
			}

			if i == 0 || lowestVersion.IsGreater(currentVersion) {
				lowestVersion = currentVersion
			}
		}

		result = lowestVersion

		return nil
	})
	if err != nil {
		return iModels.AerospikeVersion{}, err
	}

	return result, nil
}

// HasExpressionSIndex checks whether the namespace contains expression based secondary indexes.
func (ic *Client) HasExpressionSIndex(ctx context.Context, namespace string) (bool, error) {
	list, err := ic.GetSIndexes(ctx, namespace)
	if err != nil {
		return false, err
	}

	for _, idx := range list {
		if idx.Expression != "" {
			return true, nil
		}
	}

	return false, nil
}

// GetSIndexes returns list of SIndexes for the given namespace.
func (ic *Client) GetSIndexes(ctx context.Context, namespace string) ([]*models.SIndex, error) {
	var indexes []*models.SIndex

	err := executeWithRetry(ctx, ic.retryPolicy, func() error {
		node, aErr := ic.cluster.GetRandomNode()
		if aErr != nil {
			return aErr.Unwrap()
		}

		var getErr error
		indexes, getErr = ic.getSIndexes(node, namespace, ic.policy)

		return getErr
	})

	return indexes, err
}

// GetUDFs returns list of UDFs.
func (ic *Client) GetUDFs(ctx context.Context) ([]*models.UDF, error) {
	var udfs []*models.UDF

	err := executeWithRetry(ctx, ic.retryPolicy, func() error {
		node, aErr := ic.cluster.GetRandomNode()
		if aErr != nil {
			return aErr.Unwrap()
		}

		var getErr error
		udfs, getErr = ic.getUDFs(node, ic.policy)

		return getErr
	})

	return udfs, err
}

// SupportsBatchWrite reports whether the cluster version supports batch writes.
func (ic *Client) SupportsBatchWrite(ctx context.Context) (bool, error) {
	v, err := ic.GetVersion(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get aerospike version: %w", err)
	}

	return v.IsGreaterOrEqual(iModels.AerospikeVersionSupportsBatchWrites), nil
}

// GetRecordCount counts number of records in given namespace and sets.
func (ic *Client) GetRecordCount(ctx context.Context, namespace string, sets []string) (uint64, error) {
	var count uint64

	err := executeWithRetry(ctx, ic.retryPolicy, func() error {
		node, aErr := ic.cluster.GetRandomNode()
		if aErr != nil {
			return aErr
		}

		effectiveReplicationFactor, err := ic.getEffectiveReplicationFactor(node, ic.policy, namespace)
		if err != nil {
			return err
		}

		// If a database not started yet, it can respond with 0.
		if effectiveReplicationFactor == 0 {
			return ErrReplicationFactorZero
		}

		var recordsNumber uint64

		for _, node := range ic.cluster.GetNodes() {
			if !node.IsActive() {
				continue
			}

			var recordCountForNode uint64

			switch {
			case len(sets) == 0:
				recordCountForNode, err = ic.getRecordCountForNodeNamespace(node, ic.policy, namespace)
			default:
				recordCountForNode, err = ic.getRecordCountForNode(node, ic.policy, namespace, sets)
			}

			if err != nil {
				return err
			}

			recordsNumber += recordCountForNode
		}

		count = recordsNumber / uint64(effectiveReplicationFactor)

		return nil
	})

	return count, err
}

// GetPendingMigrations returns the number of pending migrations.
func (ic *Client) GetPendingMigrations(ctx context.Context, namespace string) (uint64, error) {
	var result uint64

	err := executeWithRetry(ctx, ic.retryPolicy, func() error {
		res, err := ic.getClusterTotalMigrations(namespace)
		if err != nil {
			return fmt.Errorf("failed to fetch migration stats: %w", err)
		}

		result = res

		return nil
	})

	return result, err
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

	response, aErr := node.RequestInfo(ic.policy, cmd)
	if aErr != nil {
		return 0, fmt.Errorf("failed to get request info: %w", aErr)
	}

	resultMap, err := parseInfoResponse(response[cmd], ";", ":", "=")
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

// StartXDR creates xdr config and starts replication.
func (ic *Client) StartXDR(
	ctx context.Context, nodeName, dc, hostPort, namespace, rewind string, throughput int, forward bool,
) error {
	// The order of these operations is important. Don't reorder it if you don't know what you are doing!
	steps := []func() error{
		func() error { return ic.createXDRDC(nodeName, dc) },
		func() error { return ic.createXDRConnector(nodeName, dc) },
		func() error { return ic.createXDRNode(nodeName, dc, hostPort) },
		func() error { return ic.setMaxThroughput(nodeName, dc, namespace, throughput) },
		func() error { return ic.createXDRNamespace(nodeName, dc, namespace, rewind) },
	}

	if forward {
		steps = append(steps, func() error {
			return ic.setXDRForward(nodeName, dc, namespace, forward)
		})
	}

	for _, step := range steps {
		if err := executeWithRetry(ctx, ic.retryPolicy, step); err != nil {
			return err
		}
	}

	return nil
}

// StopXDR disable replication and remove xdr config.
func (ic *Client) StopXDR(ctx context.Context, nodeName, dc string) error {
	return executeWithRetry(ctx, ic.retryPolicy, func() error {
		return ic.deleteXDRDC(nodeName, dc)
	})
}

func (ic *Client) createXDRDC(nodeName, dc string) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDCreateXDRDC], dc)
	return ic.execByNode(nodeName, cmd, "create xdr dc")
}

func (ic *Client) createXDRConnector(nodeName, dc string) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDCreateConnector], dc)
	return ic.execByNode(nodeName, cmd, "create xdr connector")
}

func (ic *Client) createXDRNode(nodeName, dc, hostPort string) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDCreateXDRNode], dc, hostPort)
	return ic.execByNode(nodeName, cmd, "create xdr node")
}

func (ic *Client) createXDRNamespace(nodeName, dc, namespace, rewind string) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDCreateXDRNamespace], dc, namespace, rewind)
	return ic.execByNode(nodeName, cmd, "create xdr namespace")
}

func (ic *Client) deleteXDRDC(nodeName, dc string) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDDeleteXDRDC], dc)
	return ic.execByNode(nodeName, cmd, "remove xdr dc")
}

// BlockMRTWrites blocks MRT writes on cluster.
func (ic *Client) BlockMRTWrites(ctx context.Context, nodeName, namespace string) error {
	return executeWithRetry(ctx, ic.retryPolicy, func() error {
		return ic.blockMRTWrites(nodeName, namespace)
	})
}

func (ic *Client) blockMRTWrites(nodeName, namespace string) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDBlockMRTWrites], namespace)
	return ic.execByNode(nodeName, cmd, "block mrt writes")
}

// UnBlockMRTWrites unblocks MRT writes on cluster.
func (ic *Client) UnBlockMRTWrites(ctx context.Context, nodeName, namespace string) error {
	return executeWithRetry(ctx, ic.retryPolicy, func() error {
		return ic.unBlockMRTWrites(nodeName, namespace)
	})
}

func (ic *Client) unBlockMRTWrites(nodeName, namespace string) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDUnBlockMRTWrites], namespace)
	return ic.execByNode(nodeName, cmd, "unblock mrt writes")
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

// setMaxThroughput sets max throughput for xdr. The value should be in multiples of 100.
func (ic *Client) setMaxThroughput(nodeName, dc, namespace string, throughput int) error {
	// Do nothing.
	if throughput == 0 {
		return nil
	}

	cmd := fmt.Sprintf(ic.cmdDict[cmdIDSetXDRMaxThroughput], dc, namespace, throughput)

	return ic.execByNode(nodeName, cmd, "set max throughput")
}

// setXDRForward setting this parameter to true sends writes,
// that originated from another XDR to the specified destination datacenters.
func (ic *Client) setXDRForward(nodeName, dc, namespace string, forward bool) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDSetXDRForward], dc, namespace, forward)

	return ic.execByNode(nodeName, cmd, "set xdr forward")
}

// GetSetsList returns the list of set names for the given namespace, excluding the MRT monitor set.
func (ic *Client) GetSetsList(ctx context.Context, namespace string) ([]string, error) {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDSetsOfNamespace], namespace)

	resp, err := ic.GetInfo(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get sets: %w", err)
	}

	result, err := parseResultResponse(cmd, resp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse sets info response: %w", err)
	}

	resultMap, err := parseInfoResponse(result, ";", ":", "=")
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

	resp, err := ic.GetInfo(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get racks info: %w", err)
	}

	result, err := parseResultResponse(cmd, resp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse racks info response: %w", err)
	}

	resultMap, err := parseInfoResponse(result, ";", ":", "=")
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

// Stats represent a result of get stats command.
// In the future, other fields can be added.
type Stats struct {
	Lag               int64
	Recoveries        int64
	RecoveriesPending int64
}

// GetStats requests node statistics like recoveries, lag, etc.
// returns Stats struct.
func (ic *Client) GetStats(ctx context.Context, nodeName, dc, namespace string) (Stats, error) {
	var result Stats

	err := executeWithRetry(ctx, ic.retryPolicy, func() error {
		res, err := ic.getStats(nodeName, dc, namespace)
		if err != nil {
			return err
		}

		result = res

		return nil
	})

	return result, err
}

func (ic *Client) getStats(nodeName, dc, namespace string) (Stats, error) {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDGetXDRStats], dc, namespace)

	resp, err := ic.requestByNode(nodeName, cmd)
	if err != nil {
		return Stats{}, fmt.Errorf("failed to get stats: %w", err)
	}

	result, err := parseResultResponse(cmd, resp)
	if err != nil {
		return Stats{}, fmt.Errorf("failed to parse get stats response: %w", err)
	}

	resultMap, err := parseInfoResponse(result, ";", ":", "=")
	if err != nil {
		return Stats{}, fmt.Errorf("failed to parse get stats response map: %w", err)
	}

	var stats Stats

	for i := range resultMap {
		lag, ok, err := resultMap[i].ParseInt64("lag")
		if err != nil {
			return Stats{}, err
		}

		if ok {
			stats.Lag = lag
		}

		recoveries, ok, err := resultMap[i].ParseInt64("recoveries")
		if err != nil {
			return Stats{}, err
		}

		if ok {
			stats.Recoveries = recoveries
		}

		recoveriesPending, ok, err := resultMap[i].ParseInt64("recoveries_pending")
		if err != nil {
			return Stats{}, err
		}

		if ok {
			stats.RecoveriesPending = recoveriesPending
		}
	}

	return stats, nil
}

// GetService returns service name by node name.
func (ic *Client) GetService(ctx context.Context, node string) (string, error) {
	var result string

	// First request TLS name.
	err := executeWithRetry(ctx, ic.retryPolicy, func() error {
		res, err := ic.getByNode(node, ic.cmdDict[cmdIDServiceTLSStd])
		if err != nil {
			return err
		}

		result = res

		return nil
	})

	// If result is empty, then request plain.
	if result == "" {
		err = executeWithRetry(ctx, ic.retryPolicy, func() error {
			res, err := ic.getByNode(node, ic.cmdDict[cmdIDServiceClearStd])
			if err != nil {
				return err
			}

			result = res

			return nil
		})
	}

	return result, err
}

func (ic *Client) getByNode(node, cmd string) (string, error) {
	resp, err := ic.requestByNode(node, cmd)
	if err != nil {
		return "", fmt.Errorf("failed to get %s info for node %s: %w", cmd, node, err)
	}

	result, err := parseResultResponse(cmd, resp)
	if err != nil {
		return "", fmt.Errorf("failed to parse %s info response: %w", cmd, err)
	}

	return result, nil
}

// GetNamespacesList returns list of namespaces.
func (ic *Client) GetNamespacesList(ctx context.Context) ([]string, error) {
	cmd := ic.cmdDict[cmdIDNamespaces]

	resp, err := ic.GetInfo(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get namespaces list: %w", err)
	}

	result, err := parseResultResponse(cmd, resp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse namespaces list response: %w", err)
	}

	return strings.Split(result, ";"), nil
}

// GetStatus returns cluster status.
func (ic *Client) GetStatus(ctx context.Context) (string, error) {
	cmd := ic.cmdDict[cmdIDStatus]

	resp, err := ic.GetInfo(ctx, cmd)
	if err != nil {
		return "", fmt.Errorf("failed to get status info: %w", err)
	}

	result, err := parseResultResponse(cmd, resp)
	if err != nil {
		return "", fmt.Errorf("failed to parse status response: %w", err)
	}

	return result, nil
}

// GetDCsList returns list of XDR DCs.
func (ic *Client) GetDCsList(ctx context.Context) ([]string, error) {
	cmd := ic.cmdDict[cmdIDGetConfigXDR]

	resp, err := ic.GetInfo(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get DCs list: %w", err)
	}

	result, err := parseResultResponse(cmd, resp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DCs list result response: %w", err)
	}

	infoResponse, err := parseInfoResponse(result, ";", ":", "=")
	if err != nil {
		return nil, fmt.Errorf("failed to parse DCs list info response: %w", err)
	}

	dcs := make([]string, 0, len(infoResponse))

	for _, rec := range infoResponse {
		val, ok := rec["dcs"]
		if !ok {
			continue
		}

		dcs = append(dcs, val)
	}

	return dcs, nil
}

// GetPrimaryPartitions returns a list of primary partitions.
func (ic *Client) GetPrimaryPartitions(ctx context.Context, node, namespace string) ([]int, error) {
	var result []int

	err := executeWithRetry(ctx, ic.retryPolicy, func() error {
		res, err := ic.getPrimaryPartitions(node, namespace)
		if err != nil {
			return err
		}

		result = res

		return nil
	})

	return result, err
}

func (ic *Client) getPrimaryPartitions(node, namespace string) ([]int, error) {
	cmd := ic.cmdDict[cmdIDReplicas]

	result, err := ic.getByNode(node, cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get by node command: %s: %w", cmd, err)
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
		return nil, fmt.Errorf("failed to find replicas for node %s", node)
	}

	bitMap, err := base64StringToBitArray(base64Res)
	if err != nil {
		return nil, fmt.Errorf("failed to parse primary partition bitmap: %w", err)
	}

	return bitMapToIntSlice(bitMap), nil
}

// StartServerBackup starts a backup job on the server.
func (ic *Client) StartServerBackup(ctx context.Context,
	namespace, storage, bucket, region, profile, accessKey, secretKey, modifiedBefore, modifiedAfter string,
) (string, error) {
	cNow := cltime.Now()
	jobID := cNow.String()

	cmd := fmt.Sprintf(ic.cmdDict[cmdIDServerBackup],
		namespace, jobID, storage, bucket, region, profile, accessKey, secretKey, modifiedBefore, modifiedAfter)

	resp, err := ic.GetInfo(ctx, cmd)
	if err != nil {
		return "", fmt.Errorf("failed start backup: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return "", fmt.Errorf("failed to parse start backup response: %w", err)
	}

	return jobID, nil
}

// StartServerRestore starts a restore job on the server.
func (ic *Client) StartServerRestore(ctx context.Context, jobID, namespace, storage, bucket, region, profile,
	accessKey, secretKey string,
) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDServerRestore],
		namespace, jobID, storage, bucket, region, profile, accessKey, secretKey)

	resp, err := ic.GetInfo(ctx, cmd)
	if err != nil {
		return fmt.Errorf("failed start restore: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse start restore response: %w", err)
	}

	return nil
}

// PrepareServerRestore starts a restore preparation on the server.
func (ic *Client) PrepareServerRestore(ctx context.Context, jobID, namespace string) error {
	allNodes := ic.getNodesString()
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDServerPrepareRestore], namespace, jobID, allNodes)

	resp, err := ic.GetInfo(ctx, cmd)
	if err != nil {
		return fmt.Errorf("failed prepare restore: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse prepare restore response: %w", err)
	}

	return nil
}

func (ic *Client) getNodesString() string {
	nodes := ic.cluster.GetNodes()

	var builder strings.Builder

	for _, node := range nodes {
		builder.WriteString(node.GetName())
		builder.WriteByte(',')
	}

	return builder.String()
}

// GetBackupStatus aggregates the server-side backup progress across all nodes and
// returns it as a ratio in the [0, 1] range.
func (ic *Client) GetBackupStatus(ctx context.Context) (float64, error) {
	var result float64

	err := executeWithRetry(ctx, ic.retryPolicy, func() error {
		nodes := ic.cluster.GetNodes()

		var (
			total     float64
			firstTrID int64
		)

		for _, node := range nodes {
			one, trID, err := ic.getBackupStatusByNode(node)
			if err != nil {
				return fmt.Errorf("failed to get backup status from node %s: %w", node.GetName(), err)
			}

			if firstTrID == 0 {
				firstTrID = trID
			}

			if trID != firstTrID {
				return fmt.Errorf("backup trid mismatch: node %s has trid %d, expected %d",
					node.GetName(), trID, firstTrID)
			}

			total += one
		}

		result = math.Min(1.0, total/partitionsPerNamespace)

		return nil
	})

	return result, err
}

func (ic *Client) getBackupStatusByNode(node infoGetter) (val float64, trID int64, err error) {
	jobs, err := ic.getBackupJobsByNode(node)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get backup jobs: %w", err)
	}

	if len(jobs) == 0 {
		return 0, 0, ErrNotFound
	}

	latestBackup := jobs[0]

	trID, okTrID, err := latestBackup.ParseInt64("trid")
	if err != nil {
		return 0, 0, err
	}

	progress, okProgress, err := latestBackup.ParseFloat64("job-progress")
	if err != nil {
		return 0, 0, err
	}

	pids, okPids, err := latestBackup.ParseFloat64("n-pids-requested")
	if err != nil {
		return 0, 0, err
	}

	if okProgress && okPids && okTrID {
		return progress / percentBase * float64(pids), trID, nil
	}

	return 0, 0, ErrNotFound
}

func (ic *Client) getBackupJobsByNode(node infoGetter) ([]iModels.InfoMap, error) {
	jobs, err := ic.getJobsQueriesByNode(node)
	if err != nil {
		return nil, fmt.Errorf("failed to get jobs: %w", err)
	}

	// Latest backup job will be first.
	jobs, err = filterBackupsSortedByTimeSinceDone(jobs)
	if err != nil {
		return nil, fmt.Errorf("failed to filter backups sorted by time since done: %w", err)
	}

	return jobs, nil
}

func (ic *Client) getUDFs(node infoGetter, policy *a.InfoPolicy) ([]*models.UDF, error) {
	cmd := ic.cmdDict[cmdIDUdfList]

	response, aErr := node.RequestInfo(policy, cmd)
	if aErr != nil {
		return nil, fmt.Errorf("failed to list UDFs: %w", aErr)
	}

	cmdResp, err := parseResultResponse(cmd, response)
	if err != nil {
		return nil, fmt.Errorf("failed to parse udf-list response: %w", err)
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

		udf, err := ic.getUDF(node, name, policy)
		if err != nil {
			return nil, fmt.Errorf("failed to get UDF: %w", err)
		}

		udfs[i] = udf
	}

	return udfs, nil
}

func (ic *Client) getUDF(node infoGetter, name string, policy *a.InfoPolicy) (*models.UDF, error) {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDUdfGetFilename], name)

	response, aErr := node.RequestInfo(policy, cmd)
	if aErr != nil {
		return nil, fmt.Errorf("udf-get info command failed: %w", aErr)
	}

	cmdResp, err := parseResultResponse(cmd, response)
	if err != nil {
		return nil, fmt.Errorf("failed to parse UDF response: %w", err)
	}

	udf, err := parseUDFResponse(cmdResp)
	if err != nil {
		return nil, err
	}

	udf.Name = name

	return udf, nil
}

func (ic *Client) getRecordCountForNode(node infoGetter, policy *a.InfoPolicy, namespace string, sets []string,
) (uint64, error) {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDSetsOfNamespace], namespace)

	response, aErr := node.RequestInfo(policy, cmd)
	if aErr != nil {
		return 0, fmt.Errorf("failed to get record count: %w", aErr)
	}

	infoResponse, err := parseInfoResponse(response[cmd], ";", ":", "=")
	if err != nil {
		return 0, fmt.Errorf("failed to parse record info request: %w", err)
	}

	var recordsNumber uint64

	for _, setInfo := range infoResponse {
		setName, ok := setInfo["set"]
		if !ok {
			return 0, fmt.Errorf("set name missing in response %s", response[cmd])
		}

		// Skip MRT monitor records.
		if setName == models.MonitorRecordsSetName {
			continue
		}

		if len(sets) == 0 || slices.Contains(sets, setName) {
			objectCount, ok := setInfo["objects"]
			if !ok {
				return 0, fmt.Errorf("objects number missing in response %s", response[cmd])
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

func (ic *Client) getRecordCountForNodeNamespace(node infoGetter, policy *a.InfoPolicy, namespace string,
) (uint64, error) {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDNamespaceInfo], namespace)

	response, aErr := node.RequestInfo(policy, cmd)
	if aErr != nil {
		return 0, fmt.Errorf("failed to request info: %w", aErr)
	}

	resultMap, err := parseInfoResponse(response[cmd], ";", ":", "=")
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

func (ic *Client) getEffectiveReplicationFactor(node infoGetter, policy *a.InfoPolicy, namespace string,
) (int, error) {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDNamespaceInfo], namespace)

	response, aErr := node.RequestInfo(policy, cmd)
	if aErr != nil {
		return 0, fmt.Errorf("failed to get namespace info: %w", aErr)
	}

	infoResponse, err := parseInfoResponse(response[cmd], ";", ":", "=")
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

func (ic *Client) getJobsQueriesByNode(node infoGetter) ([]iModels.InfoMap, error) {
	cmd := ic.cmdDict[cmdIDShowJobsQueries]

	response, aErr := node.RequestInfo(ic.policy, cmd)
	if aErr != nil {
		return nil, fmt.Errorf("failed to show job queries: %w", aErr)
	}

	infoResponse, err := parseInfoResponse(response[cmd], ";", ":", "=")
	if err != nil {
		return nil, fmt.Errorf("failed to parse show job info response: %w", err)
	}

	return infoResponse, nil
}

// GetClusterStable checks the stability of a cluster within the specified namespace and retries on transient errors.
// Returns a boolean indicating the stability status and an error if the operation fails after retries.
func (ic *Client) GetClusterStable(ctx context.Context, namespace string) (bool, error) {
	var result bool

	err := executeWithRetry(ctx, ic.retryPolicy, func() error {
		res, err := ic.getClusterStable(ctx, namespace)
		if err != nil {
			return err
		}

		result = res

		return nil
	})

	return result, err
}

func (ic *Client) getClusterStable(ctx context.Context, namespace string) (bool, error) {
	nodes := ic.cluster.GetNodes()
	nodesNum := len(nodes)

	stats, err := ic.getStatistics(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get cluster statistics: %w", err)
	}

	clusterKey, ok := searchInInfoResponse(stats, "cluster_key")
	if !ok {
		return false, fmt.Errorf("cluster key not found in statistics")
	}

	for _, node := range nodes {
		cmd := fmt.Sprintf(ic.cmdDict[cmdIDClusterStable], nodesNum, namespace)

		resp, err := ic.GetInfo(ctx, cmd)
		if err != nil {
			return false, fmt.Errorf("failed to get node %s stable status: %w", node.GetName(), err)
		}

		result, err := parseResultResponse(cmd, resp)
		if err != nil {
			return false, fmt.Errorf("failed to parse node %s stable status response: %w", node.GetName(), err)
		}

		if result != clusterKey {
			return false, fmt.Errorf("cluster %s is not stable, result is %s", clusterKey, result)
		}
	}

	return true, nil
}

// GetStatistics returns cluster statistics.
func (ic *Client) getStatistics(ctx context.Context) ([]iModels.InfoMap, error) {
	cmd := ic.cmdDict[cmdIDStatistics]

	resp, err := ic.GetInfo(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster statistics: %w", err)
	}

	result, err := parseResultResponse(cmd, resp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cluster statistics result response: %w", err)
	}

	infoResponse, err := parseInfoResponse(result, ";", ";", "=")
	if err != nil {
		return nil, fmt.Errorf("failed to parse cluster statistics info response: %w", err)
	}

	return infoResponse, nil
}

func searchInInfoResponse(infoResponse []iModels.InfoMap, key string) (string, bool) {
	for _, r := range infoResponse {
		val, ok := r[key]
		if ok {
			return val, true
		}
	}

	return "", false
}
