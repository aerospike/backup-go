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
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
	"github.com/segmentio/asm/base64"
)

const errCmdRespPrefix = "ERROR"

var (
	aerospikeVersionRegex = regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)`)
	secretAgentValRegex   = regexp.MustCompile(`(.+?)=secrets:(.+?):(.+?)`)
)

type AerospikeVersion struct {
	Major int
	Minor int
	Patch int
}

var (
	AerospikeVersionSupportsSIndexContext = AerospikeVersion{6, 1, 0}
	AerospikeVersionSupportsBatchWrites   = AerospikeVersion{6, 0, 0}
	// AerospikeVersionRecentInfoCommands after this version, all commands should use
	// `namespace` parameter instead of `ns` or `id`.
	AerospikeVersionRecentInfoCommands = AerospikeVersion{8, 1, 0}
)

var (
	ErrReplicationFactorZero = errors.New("replication factor is zero")
)

func (av AerospikeVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", av.Major, av.Minor, av.Patch)
}

func (av AerospikeVersion) IsGreater(other AerospikeVersion) bool {
	if av.Major > other.Major {
		return true
	}

	if av.Major == other.Major {
		if av.Minor > other.Minor {
			return true
		}

		if av.Minor == other.Minor {
			if av.Patch > other.Patch {
				return true
			}
		}
	}

	return false
}

func (av AerospikeVersion) IsGreaterOrEqual(other AerospikeVersion) bool {
	return av.IsGreater(other) || av == other
}

// infoGetter defines the methods for doing info requests
// with the Aerospike database.
//
//go:generate mockery --name InfoGetter
type infoGetter interface {
	RequestInfo(infoPolicy *a.InfoPolicy, commands ...string) (map[string]string, a.Error)
}

type nodeGetter interface {
	GetRandomNode() (*a.Node, a.Error)
	GetNodeByName(name string) (*a.Node, a.Error)
	GetNodes() []*a.Node
}

// Client manages asinfo interactions with an Aerospike cluster, handling policies, retry logic, and command operations.
type Client struct {
	policy      *a.InfoPolicy
	cluster     nodeGetter
	retryPolicy *models.RetryPolicy
	cmdDict     map[int]string
}

// NewClient initializes and returns a new asinfo Client instance with the provided Aerospike client,
// policy, and retry policy.
func NewClient(
	cluster nodeGetter,
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

	v, err := ic.GetVersion()
	if err != nil {
		return nil, fmt.Errorf("failed to get aerospike version: %w", err)
	}

	ic.cmdDict = newCmdDict(v)

	return ic, nil
}

func (ic *Client) GetInfo(names ...string) (map[string]string, error) {
	var result map[string]string

	err := executeWithRetry(ic.retryPolicy, func() error {
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

func (ic *Client) GetVersion() (AerospikeVersion, error) {
	var (
		version AerospikeVersion
		err     error
	)

	err = executeWithRetry(ic.retryPolicy, func() error {
		node, aErr := ic.cluster.GetRandomNode()
		if aErr != nil {
			return aErr.Unwrap()
		}

		version, err = ic.getAerospikeVersion(node, ic.policy)

		return err
	})

	return version, err
}

func (ic *Client) GetSIndexes(namespace string) ([]*models.SIndex, error) {
	var (
		indexes []*models.SIndex
		err     error
	)

	err = executeWithRetry(ic.retryPolicy, func() error {
		node, aErr := ic.cluster.GetRandomNode()
		if aErr != nil {
			return aErr.Unwrap()
		}

		indexes, err = ic.getSIndexes(node, namespace, ic.policy)

		return err
	})

	return indexes, err
}

func (ic *Client) GetUDFs() ([]*models.UDF, error) {
	var (
		udfs []*models.UDF
		err  error
	)

	err = executeWithRetry(ic.retryPolicy, func() error {
		node, aErr := ic.cluster.GetRandomNode()
		if aErr != nil {
			return aErr.Unwrap()
		}

		udfs, err = ic.getUDFs(node, ic.policy)

		return err
	})

	return udfs, err
}

func (ic *Client) SupportsBatchWrite() (bool, error) {
	var supports bool

	err := executeWithRetry(ic.retryPolicy, func() error {
		version, err := ic.GetVersion()
		if err != nil {
			return fmt.Errorf("failed to get aerospike version: %w", err)
		}

		supports = version.IsGreaterOrEqual(AerospikeVersionSupportsBatchWrites)

		return nil
	})

	return supports, err
}

// GetRecordCount counts number of records in given namespace and sets.
func (ic *Client) GetRecordCount(namespace string, sets []string) (uint64, error) {
	var count uint64

	err := executeWithRetry(ic.retryPolicy, func() error {
		node, aerr := ic.cluster.GetRandomNode()
		if aerr != nil {
			return aerr
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

// StartXDR creates xdr config and starts replication.
func (ic *Client) StartXDR(nodeName, dc, hostPort, namespace, rewind string, throughput int, forward bool) error {
	// The Order of this operation is important. Don't move it if you don't know what you are doing!
	if err := executeWithRetry(
		ic.retryPolicy,
		func() error {
			return ic.createXDRDC(nodeName, dc)
		},
	); err != nil {
		return err
	}

	if err := executeWithRetry(
		ic.retryPolicy,
		func() error {
			return ic.createXDRConnector(nodeName, dc)
		},
	); err != nil {
		return err
	}

	if err := executeWithRetry(
		ic.retryPolicy,
		func() error {
			return ic.createXDRNode(nodeName, dc, hostPort)
		},
	); err != nil {
		return err
	}

	if err := executeWithRetry(
		ic.retryPolicy,
		func() error {
			return ic.setMaxThroughput(nodeName, dc, namespace, throughput)
		},
	); err != nil {
		return err
	}

	if err := executeWithRetry(
		ic.retryPolicy,
		func() error {
			return ic.createXDRNamespace(nodeName, dc, namespace, rewind)
		},
	); err != nil {
		return err
	}

	if forward {
		if err := executeWithRetry(
			ic.retryPolicy,
			func() error {
				return ic.setXDRForward(nodeName, dc, namespace, forward)
			},
		); err != nil {
			return err
		}
	}

	return nil
}

// StopXDR disable replication and remove xdr config.
func (ic *Client) StopXDR(nodeName, dc string) error {
	return executeWithRetry(ic.retryPolicy, func() error {
		return ic.stopXDR(nodeName, dc)
	})
}

func (ic *Client) stopXDR(nodeName, dc string) error {
	if err := ic.deleteXDRDC(nodeName, dc); err != nil {
		return err
	}

	return nil
}

func (ic *Client) createXDRDC(nodeName, dc string) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDCreateXDRDC], dc)

	resp, err := ic.requestByNode(nodeName, cmd)
	if err != nil {
		return fmt.Errorf("failed to create xdr dc: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse create xdr dc response: %w", err)
	}

	return nil
}

func (ic *Client) createXDRConnector(nodeName, dc string) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDCreateConnector], dc)

	resp, err := ic.requestByNode(nodeName, cmd)
	if err != nil {
		return fmt.Errorf("failed to create xdr connector: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse create xdr connector response: %w", err)
	}

	return nil
}

func (ic *Client) createXDRNode(nodeName, dc, hostPort string) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDCreateXDRNode], dc, hostPort)

	resp, err := ic.requestByNode(nodeName, cmd)
	if err != nil {
		return fmt.Errorf("failed to create xdr node: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse create xdr node response: %w", err)
	}

	return nil
}

func (ic *Client) createXDRNamespace(nodeName, dc, namespace, rewind string) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDCreateXDRNamespace], dc, namespace, rewind)

	resp, err := ic.requestByNode(nodeName, cmd)
	if err != nil {
		return fmt.Errorf("failed to create xdr namesapce: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse create xdr namesapce response: %w", err)
	}

	return nil
}

func (ic *Client) deleteXDRDC(nodeName, dc string) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDDeleteXDRDC], dc)

	resp, err := ic.requestByNode(nodeName, cmd)
	if err != nil {
		return fmt.Errorf("failed to remove xdr dc: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse remove xdr dc response: %w", err)
	}

	return nil
}

// BlockMRTWrites blocks MRT writes on cluster.
func (ic *Client) BlockMRTWrites(nodeName, namespace string) error {
	return executeWithRetry(ic.retryPolicy, func() error {
		return ic.blockMRTWrites(nodeName, namespace)
	})
}

func (ic *Client) blockMRTWrites(nodeName, namespace string) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDBlockMRTWrites], namespace)

	resp, err := ic.requestByNode(nodeName, cmd)
	if err != nil {
		return fmt.Errorf("failed to block mrt writes: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse block mrt writes response: %w", err)
	}

	return nil
}

// UnBlockMRTWrites unblocks MRT writes on cluster.
func (ic *Client) UnBlockMRTWrites(nodeName, namespace string) error {
	return executeWithRetry(ic.retryPolicy, func() error {
		return ic.unBlockMRTWrites(nodeName, namespace)
	})
}

func (ic *Client) unBlockMRTWrites(nodeName, namespace string) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDUnBlockMRTWrites], namespace)

	resp, err := ic.requestByNode(nodeName, cmd)
	if err != nil {
		return fmt.Errorf("failed to unblock mrt writes: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse unblock mrt writes response: %w", err)
	}

	return nil
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

// SetMaxThroughput sets max throughput for xdr. The Value should be in multiples of 100
func (ic *Client) setMaxThroughput(nodeName, dc, namespace string, throughput int) error {
	// Do nothing.
	if throughput == 0 {
		return nil
	}

	cmd := fmt.Sprintf(ic.cmdDict[cmdIDSetXDRMaxThroughput], dc, namespace, throughput)

	resp, err := ic.requestByNode(nodeName, cmd)
	if err != nil {
		return fmt.Errorf("failed to set max throughput: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse set max throughput response: %w", err)
	}

	return nil
}

// setXDRForward setting this parameter to true sends writes,
// that originated from another XDR to the specified destination datacenters.
func (ic *Client) setXDRForward(nodeName, dc, namespace string, forward bool) error {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDSetXDRForward], dc, namespace, forward)

	resp, err := ic.requestByNode(nodeName, cmd)
	if err != nil {
		return fmt.Errorf("failed to set xdr forward: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse set xdr forward response: %w", err)
	}

	return nil
}

func (ic *Client) GetSetsList(namespace string) ([]string, error) {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDSetsOfNamespace], namespace)

	resp, err := ic.GetInfo(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed get sets: %w", err)
	}

	result, err := parseResultResponse(cmd, resp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse sets info response: %w", err)
	}

	resultMap, err := parseInfoResponse(result, ";", ":", "=")
	if err != nil {
		return nil, fmt.Errorf("failed to parse sets info: %w", err)
	}

	sets := make([]string, 0)

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
func (ic *Client) GetRackNodes(rackID int) ([]string, error) {
	var (
		result []string
		err    error
	)

	err = executeWithRetry(ic.retryPolicy, func() error {
		result, err = ic.getRackNodes(rackID)
		if err != nil {
			return err
		}

		return err
	})

	return result, err
}

// getRackNodes returns list of nodes for a rack.
func (ic *Client) getRackNodes(rackID int) ([]string, error) {
	cmd := ic.cmdDict[cmdIDRack]

	resp, err := ic.GetInfo(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed get racks info: %w", err)
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

	for _, v := range resultMap {
		for n, m := range v {
			if strings.HasPrefix(fmt.Sprintf("rack_%d", rackID), n) {
				nodes = strings.Split(m, ",")
			}
		}
	}

	if len(nodes) == 0 {
		return nil, fmt.Errorf("failed to find nodes for rack %d", rackID)
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
func (ic *Client) GetStats(nodeName, dc, namespace string) (Stats, error) {
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
		return Stats{}, fmt.Errorf("failed to parse to map get stats response: %w", err)
	}

	var stats Stats

	for i := range resultMap {
		if val, ok := resultMap[i]["lag"]; ok {
			stats.Lag, err = strconv.ParseInt(val, 10, 64)
			if err != nil {
				return Stats{}, fmt.Errorf("failed to parse lag: %w", err)
			}
		}

		if val, ok := resultMap[i]["recoveries"]; ok {
			stats.Recoveries, err = strconv.ParseInt(val, 10, 64)
			if err != nil {
				return Stats{}, fmt.Errorf("failed to parse recoveries: %w", err)
			}
		}

		if val, ok := resultMap[i]["recoveries_pending"]; ok {
			stats.RecoveriesPending, err = strconv.ParseInt(val, 10, 64)
			if err != nil {
				return Stats{}, fmt.Errorf("failed to parse recoveries_pending: %w", err)
			}
		}
	}

	return stats, nil
}

// GetService returns service name by node name.
func (ic *Client) GetService(node string) (string, error) {
	var (
		result string
		err    error
	)
	// First request TLS name.
	err = executeWithRetry(ic.retryPolicy, func() error {
		result, err = ic.getService(node, ic.cmdDict[cmdIDServiceTLSStd])
		if err != nil {
			return err
		}

		return err
	})
	// If result is empty, then request plain.
	if result == "" {
		err = executeWithRetry(ic.retryPolicy, func() error {
			result, err = ic.getService(node, ic.cmdDict[cmdIDServiceClearStd])
			if err != nil {
				return err
			}

			return err
		})
	}

	return result, err
}

func (ic *Client) getService(node, cmd string) (string, error) {
	resp, err := ic.requestByNode(node, cmd)
	if err != nil {
		return "", fmt.Errorf("failed get %s info for node %s: %w", cmd, node, err)
	}

	result, err := parseResultResponse(cmd, resp)
	if err != nil {
		return "", fmt.Errorf("failed to parse %s info response: %w", cmd, err)
	}

	return result, nil
}

// GetNamespacesList returns list of namespaces.
func (ic *Client) GetNamespacesList() ([]string, error) {
	cmd := ic.cmdDict[cmdIDNamespaces]

	resp, err := ic.GetInfo(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed get namespaces list: %w", err)
	}

	result, err := parseResultResponse(cmd, resp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse namespaces list response: %w", err)
	}

	return strings.Split(result, ";"), nil
}

// GetStatus returns cluster status.
func (ic *Client) GetStatus() (string, error) {
	cmd := ic.cmdDict[cmdIDStatus]

	resp, err := ic.GetInfo(cmd)
	if err != nil {
		return "", fmt.Errorf("failed get status info: %w", err)
	}

	result, err := parseResultResponse(cmd, resp)
	if err != nil {
		return "", fmt.Errorf("failed to parse status response: %w", err)
	}

	return result, nil
}

// GetDCsList returns list of DCs
func (ic *Client) GetDCsList() ([]string, error) {
	cmd := ic.cmdDict[cmdIDGetConfigXDR]

	resp, err := ic.GetInfo(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed get DCs list: %w", err)
	}

	result, err := parseResultResponse(cmd, resp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DCs list result response: %w", err)
	}

	fmt.Println(result)

	infoResponse, err := parseInfoResponse(result, ";", ":", "=")
	if err != nil {
		return nil, fmt.Errorf("failed to parse DCs list info response: %w", err)
	}

	dcs := make([]string, 0)

	for _, rec := range infoResponse {
		val, ok := rec["dcs"]
		if !ok {
			continue
		}

		dcs = append(dcs, val)
	}

	fmt.Println(infoResponse)

	return dcs, nil
}

// ***** Utility functions *****

func parseResultResponse(cmd string, result map[string]string) (string, error) {
	v, ok := result[cmd]
	if !ok {
		return "", fmt.Errorf("no response for command %s", cmd)
	}

	if strings.Contains(v, errCmdRespPrefix) {
		return "", fmt.Errorf("command %s failed: %s", cmd, v)
	}

	return v, nil
}

func (ic *Client) getSIndexes(node infoGetter, namespace string, policy *a.InfoPolicy) ([]*models.SIndex, error) {
	supportsSIndexCTX := AerospikeVersionSupportsSIndexContext
	version, err := ic.getAerospikeVersion(node, policy)

	if err != nil {
		return nil, fmt.Errorf("failed to get aerospike version: %w", err)
	}

	getCtx := version.IsGreaterOrEqual(supportsSIndexCTX)
	cmd := ic.buildSindexCmd(namespace, getCtx)

	response, err := node.RequestInfo(policy, cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get sindexes: %w", err)
	}

	cmdResp, err := parseResultResponse(cmd, response)
	if err != nil {
		return nil, fmt.Errorf("failed to parse sindexes response: %w", err)
	}

	return parseSIndexes(cmdResp)
}

func (ic *Client) buildSindexCmd(namespace string, getCtx bool) string {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDSindexList], namespace)

	// NOTE: getting the sindex ctx was added in Aerospike 6.1
	// so don't include this in the command at all if the server is older
	if getCtx {
		cmd += ";b64=true"
	}

	return cmd
}

func (ic *Client) getAerospikeVersion(conn infoGetter, policy *a.InfoPolicy) (AerospikeVersion, error) {
	// As we need to check version before we form dict, this command will be loaded directly.
	cmd := cmdBuild

	versionResp, aErr := conn.RequestInfo(policy, cmd)
	if aErr != nil {
		return AerospikeVersion{}, aErr
	}

	versionStr, err := parseResultResponse(cmd, versionResp)
	if err != nil {
		return AerospikeVersion{}, fmt.Errorf("failed to parse get version dc response: %w", err)
	}

	return parseAerospikeVersion(versionStr)
}

func parseAerospikeVersion(versionStr string) (AerospikeVersion, error) {
	matches := aerospikeVersionRegex.FindStringSubmatch(versionStr)
	if len(matches) != 4 {
		return AerospikeVersion{}, fmt.Errorf("failed to parse Aerospike version from '%s'", versionStr)
	}

	major, err := strconv.Atoi(matches[1])
	if err != nil {
		return AerospikeVersion{}, fmt.Errorf("failed to parse Aerospike major version %w", err)
	}

	minor, err := strconv.Atoi(matches[2])
	if err != nil {
		return AerospikeVersion{}, fmt.Errorf("failed to parse Aerospike minor version %w", err)
	}

	patch, err := strconv.Atoi(matches[3])
	if err != nil {
		return AerospikeVersion{}, fmt.Errorf("failed to parse Aerospike patch version %w", err)
	}

	return AerospikeVersion{
		Major: major,
		Minor: minor,
		Patch: patch,
	}, nil
}

func parseSIndexes(sindexListInfoResp string) ([]*models.SIndex, error) {
	sindexInfo, err := parseSindexListResponse(sindexListInfoResp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse sindex response: %w", err)
	}

	// No sindexes
	if sindexInfo == nil {
		return nil, nil
	}

	sindexes := make([]*models.SIndex, len(sindexInfo))

	for i, sindexStr := range sindexInfo {
		sindex, err := parseSIndex(sindexStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse sindex: %w", err)
		}

		sindexes[i] = sindex
	}

	return sindexes, nil
}

// parseSIndex parses a single infoMap containing a sindex into a SecondaryIndex model
func parseSIndex(sindexMap infoMap) (*models.SIndex, error) {
	si := &models.SIndex{}

	if val, ok := sindexMap["ns"]; ok {
		si.Namespace = val
	} else {
		return nil, fmt.Errorf("sindex missing namespace")
	}

	if val, ok := sindexMap["set"]; ok {
		// "NULL" is the server's representation of an empty set
		// in the sindex list info response
		if !strings.EqualFold(val, "null") {
			si.Set = val
		}
	}

	if val, ok := sindexMap["indexname"]; ok {
		si.Name = val
	} else {
		return nil, fmt.Errorf("sindex missing indexname")
	}

	if val, ok := sindexMap["indextype"]; ok {
		var sindexType models.SIndexType

		switch strings.ToLower(val) {
		case "default", "none":
			sindexType = models.BinSIndex
		case "list":
			sindexType = models.ListElementSIndex
		case "mapkeys":
			sindexType = models.MapKeySIndex
		case "mapvalues":
			sindexType = models.MapValueSIndex
		default:
			return nil, fmt.Errorf("invalid sindex index type: %s", val)
		}

		si.IndexType = sindexType
	} else {
		return nil, fmt.Errorf("sindex missing indextype")
	}

	if val, ok := sindexMap["bin"]; ok {
		path := models.SIndexPath{
			BinName: val,
		}

		if val, ok := sindexMap["type"]; ok {
			var binType models.SIPathBinType

			switch strings.ToLower(val) {
			case "numeric", "int signed":
				binType = models.NumericSIDataType
			case "string", "text":
				binType = models.StringSIDataType
			case "blob":
				binType = models.BlobSIDataType
			case "geo2dsphere", "geojson":
				binType = models.GEO2DSphereSIDataType
			default:
				return nil, fmt.Errorf("invalid sindex type: %s", val)
			}

			path.BinType = binType
		} else {
			return nil, fmt.Errorf("sindex missing type")
		}

		if val, ok := sindexMap["context"]; ok {
			// "NULL" is the server's representation of an empty context
			// in the sindex list info response
			if !strings.EqualFold(val, "null") {
				path.B64Context = val
			}
		}

		si.Path = path
	} else {
		return nil, fmt.Errorf("sindex missing bin")
	}

	// Set index expression value
	if val, ok := sindexMap["exp"]; ok {
		if strings.EqualFold(val, "null") {
			val = ""
		}

		si.Expression = val
	}

	return si, nil
}

func (ic *Client) getUDFs(node infoGetter, policy *a.InfoPolicy) ([]*models.UDF, error) {
	cmd := ic.cmdDict[cmdIDUdfList]

	response, aerr := node.RequestInfo(policy, cmd)
	if aerr != nil {
		return nil, fmt.Errorf("failed to list UDFs: %w", aerr)
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
			return nil, fmt.Errorf("udf-list response missing filename")
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

	response, aerr := node.RequestInfo(policy, cmd)
	if aerr != nil {
		return nil, fmt.Errorf("udf-get info command failed: %w", aerr)
	}

	cmdResp, err := parseResultResponse(cmd, response)
	if err != nil {
		return nil, fmt.Errorf("failed to parse udf response: %w", err)
	}

	udf, err := parseUDFResponse(cmdResp)
	if err != nil {
		return nil, err
	}

	udf.Name = name

	return udf, nil
}

func parseUDFResponse(udfGetInfoResp string) (*models.UDF, error) {
	udfInfo, err := parseUDFGetResponse(udfGetInfoResp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse udf response: %w", err)
	}

	udf, err := parseUDF(udfInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to parse udf: %w", err)
	}

	return udf, nil
}

func (ic *Client) getRecordCountForNode(node infoGetter, policy *a.InfoPolicy, namespace string, sets []string,
) (uint64, error) {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDSetsOfNamespace], namespace)

	response, aerr := node.RequestInfo(policy, cmd)
	if aerr != nil {
		return 0, fmt.Errorf("failed to get record count: %w", aerr)
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

		if len(sets) == 0 || contains(sets, setName) {
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

	response, aerr := node.RequestInfo(policy, cmd)
	if aerr != nil {
		return 0, fmt.Errorf("failed to get record count: %w", aerr)
	}

	resultMap, err := parseInfoResponse(response[cmd], ";", ":", "=")
	if err != nil {
		return 0, fmt.Errorf("failed to parse record info request: %w", err)
	}

	for i := range resultMap {
		if val, ok := resultMap[i]["objects"]; ok {
			result, err := strconv.ParseUint(val, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("failed to parse objects count: %w", err)
			}

			return result, nil
		}
	}

	return 0, fmt.Errorf("failed to parse record info request")
}

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

func (ic *Client) getEffectiveReplicationFactor(node infoGetter, policy *a.InfoPolicy, namespace string,
) (int, error) {
	cmd := fmt.Sprintf(ic.cmdDict[cmdIDNamespaceInfo], namespace)

	response, aerr := node.RequestInfo(policy, cmd)
	if aerr != nil {
		return 0, fmt.Errorf("failed to get namespace info: %w", aerr)
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

	return 0, errors.New("replication factor not found")
}

func parseUDF(udfMap infoMap) (*models.UDF, error) {
	var (
		udf     models.UDF
		udfLang string
	)

	if val, ok := udfMap["type"]; ok {
		udfLang = val
	} else {
		return nil, fmt.Errorf("udf info response missing language type")
	}

	if strings.EqualFold(udfLang, "lua") {
		udf.UDFType = models.UDFTypeLUA
	} else {
		return nil, fmt.Errorf("invalid udf language type: %s", udfLang)
	}

	if val, ok := udfMap["content"]; ok {
		// the udf content field is base64 encoded in info responses
		content, err := base64.StdEncoding.DecodeString(val)
		if err != nil {
			return nil, fmt.Errorf("failed to decode udf content: %w", err)
		}

		udf.Content = content
	} else {
		return nil, fmt.Errorf("udf info response missing content")
	}

	return &udf, nil
}

type infoMap map[string]string

// parseInfoResponse parses a single info response format string.
// the string may contain multiple response objects each separated by a semicolon
// each key-value pair is separated by a colon and the key is separated from the value by an equals sign
// e.g. "foo=bar:baz=qux;foo=bar:baz=qux"
// the above example is returned as []infoMap{infoMap{"foo": "bar", "baz": "qux"}, infoMap{"foo": "bar", "baz": "qux"}}
// if the passed in infor response is empty nil, nil is returned
func parseInfoResponse(resp, objSep, pairSep, kvSep string) ([]infoMap, error) {
	if resp == "" {
		return nil, nil
	}

	// remove the trailing object separator if it exists
	if strings.HasSuffix(resp, objSep) {
		resp = resp[:len(resp)-1]
	}

	if resp == "" {
		return nil, nil
	}

	objects := strings.Split(resp, objSep)
	info := make([]infoMap, len(objects))

	for i, object := range objects {
		data, err := parseInfoObject(object, pairSep, kvSep)
		if err != nil {
			return nil, err
		}

		info[i] = data
	}

	return info, nil
}

func parseInfoObject(obj, pairSep, kvSep string) (infoMap, error) {
	if obj == "" {
		return nil, nil
	}

	// remove the trailing object separator if it exists
	if strings.HasSuffix(obj, pairSep) {
		obj = obj[:len(obj)-1]
	}

	if obj == "" {
		return nil, nil
	}

	data := map[string]string{}

	var kvpairs []string

	switch secretAgentValRegex.MatchString(obj) {
	case true:
		// If parameter is configured with secret agent, we don't split it.
		kvpairs = append(kvpairs, obj)
	case false:
		kvpairs = strings.Split(obj, pairSep)
	}

	for _, pair := range kvpairs {
		key, val, err := parseInfoKVPair(pair, kvSep)
		if err != nil {
			return nil, err
		}

		data[key] = val
	}

	return data, nil
}

func parseInfoKVPair(pair, kvSep string) (key, val string, err error) {
	// some info key value pairs can contain kvSep in the value
	// for example, the base64 encoded context for a secondary index can contain "="
	// so we need to split on the first separator only
	kv := strings.SplitN(pair, kvSep, 2)
	if len(kv) != 2 {
		return "", "", fmt.Errorf("invalid key-value pair: %s", pair)
	}

	// make keys case-insensitive
	// to help with different version compatibility
	key = strings.ToLower(kv[0])
	val = kv[1]

	return key, val, err
}

// parseSindexListResponse parses a sindex-list info response
// example resp: ns=source-ns1:indexname=idx_timestamp:set=metrics:bin=timestamp:type=numeric:indextype=default
func parseSindexListResponse(resp string) ([]infoMap, error) {
	return parseInfoResponse(resp, ";", ":", "=")
}

// parseUDFListResponse parses a udf-list info response
// example resp: filename=basic_udf.lua,hash=706c57cb29e027221560a3cb4b693573ada98bf2,type=LUA;...
func parseUDFListResponse(resp string) ([]infoMap, error) {
	return parseInfoResponse(resp, ";", ",", "=")
}

// parseUDFGetResponse parses a udf-get info response
// example resp: type=LUA;content=LS0gQSB2ZXJ5IHNpbXBsZSBhcml0
func parseUDFGetResponse(resp string) (infoMap, error) {
	return parseInfoObject(resp, ";", "=")
}

func executeWithRetry(policy *models.RetryPolicy, command func() error) error {
	if policy == nil {
		return fmt.Errorf("retry policy cannot be nil")
	}

	var err error
	for i := range policy.MaxRetries {
		err = command()
		if err == nil {
			return nil
		}

		duration := time.Duration(float64(policy.BaseTimeout) * math.Pow(policy.Multiplier, float64(i)))
		time.Sleep(duration)
	}

	if err != nil {
		return fmt.Errorf("after %d attempts: %w", policy.MaxRetries, err)
	}

	return nil
}
