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
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/models"
)

const (
	cmdCreateXDRDC        = "set-config:context=xdr;dc=%s;action=create"
	cmdCreateConnector    = "set-config:context=xdr;dc=%s;connector=true"
	cmdCreateXDRNode      = "set-config:context=xdr;dc=%s;node-address-port=%s;action=add"
	cmdCreateXDRNamespace = "set-config:context=xdr;dc=%s;namespace=%s;action=add;rewind=%s"

	cmdRemoveXDRNamespace = "set-config:context=xdr;dc=%s;namespace=%s;action=remove"
	cmdRemoveXDRNode      = "set-config:context=xdr;dc=%s;node-address-port=%s;action=remove"
	cmdRemoveXDRDC        = "set-config:context=xdr;dc=%s;action=remove"

	cmdGetStats = "get-stats:context=xdr;dc=%s;namespace=%s"

	cmdBlockMRTWrites   = "set-config:context=namespace;id=%s;disable-mrt-writes=true"
	cmdUnBlockMRTWrites = "set-config:context=namespace;id=%s;disable-mrt-writes=false"

	cmdRespErrPrefix = "ERROR"
)

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

type aerospikeClient interface {
	Cluster() *a.Cluster
}

type InfoClient struct {
	policy  *a.InfoPolicy
	cluster *a.Cluster
}

func NewInfoClientFromAerospike(aeroClient aerospikeClient, policy *a.InfoPolicy) *InfoClient {
	return &InfoClient{
		cluster: aeroClient.Cluster(),
		policy:  policy,
	}
}

func (ic *InfoClient) GetInfo(names ...string) (map[string]string, error) {
	node, err := ic.cluster.GetRandomNode()
	if err != nil {
		return nil, err
	}

	return node.RequestInfo(ic.policy, names...)
}

func (ic *InfoClient) GetVersion() (AerospikeVersion, error) {
	node, err := ic.cluster.GetRandomNode()
	if err != nil {
		return AerospikeVersion{}, err
	}

	return getAerospikeVersion(node, ic.policy)
}

func (ic *InfoClient) GetSIndexes(namespace string) ([]*models.SIndex, error) {
	node, err := ic.cluster.GetRandomNode()
	if err != nil {
		return nil, err
	}

	return getSIndexes(node, namespace, ic.policy)
}

func (ic *InfoClient) GetUDFs() ([]*models.UDF, error) {
	node, err := ic.cluster.GetRandomNode()
	if err != nil {
		return nil, err
	}

	return getUDFs(node, ic.policy)
}

func (ic *InfoClient) SupportsBatchWrite() (bool, error) {
	version, err := ic.GetVersion()
	if err != nil {
		return false, fmt.Errorf("failed to get aerospike version: %w", err)
	}

	return version.IsGreaterOrEqual(AerospikeVersionSupportsBatchWrites), nil
}

// GetRecordCount counts number of records in given namespace and sets.
func (ic *InfoClient) GetRecordCount(namespace string, sets []string) (uint64, error) {
	node, aerr := ic.cluster.GetRandomNode()
	if aerr != nil {
		return 0, aerr
	}

	effectiveReplicationFactor, err := getEffectiveReplicationFactor(node, ic.policy, namespace)
	if err != nil {
		return 0, err
	}

	var recordsNumber uint64

	for _, node := range ic.cluster.GetNodes() {
		if !node.IsActive() {
			continue
		}

		recordCountForNode, err := getRecordCountForNode(node, ic.policy, namespace, sets)
		if err != nil {
			return 0, err
		}

		recordsNumber += recordCountForNode
	}

	return recordsNumber / uint64(effectiveReplicationFactor), nil
}

// StartXDR creates xdr config and starts replication.
func (ic *InfoClient) StartXDR(dc, hostPort, namespace, rewind string) error {
	if err := ic.createXDRDC(dc); err != nil {
		return err
	}
	// The Order of this operation is important. Don't move it if you don't know what you are doing!
	if err := ic.createXDRConnector(dc); err != nil {
		return err
	}

	if err := ic.createXDRNode(dc, hostPort); err != nil {
		return err
	}

	if err := ic.createXDRNamespace(dc, namespace, rewind); err != nil {
		return err
	}

	return nil
}

// StopXDR disable replication and remove xdr config.

func (ic *InfoClient) StopXDR(dc, hostPort, namespace string) error {
	if err := ic.removeXDRNamespace(dc, namespace); err != nil {
		return err
	}

	if err := ic.removeXDRNode(dc, hostPort); err != nil {
		return err
	}

	if err := ic.removeXDRDC(dc); err != nil {
		return err
	}

	return nil
}

func (ic *InfoClient) createXDRDC(dc string) error {
	cmd := fmt.Sprintf(cmdCreateXDRDC, dc)

	resp, err := ic.GetInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to create xdr dc: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse create xdr dc response: %w", err)
	}

	return nil
}

func (ic *InfoClient) createXDRConnector(dc string) error {
	cmd := fmt.Sprintf(cmdCreateConnector, dc)

	resp, err := ic.GetInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to create xdr connector: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse create xdr connector response: %w", err)
	}

	return nil
}

func (ic *InfoClient) createXDRNode(dc, hostPort string) error {
	cmd := fmt.Sprintf(cmdCreateXDRNode, dc, hostPort)

	resp, err := ic.GetInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to create xdr node: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse create xdr node response: %w", err)
	}

	return nil
}

func (ic *InfoClient) createXDRNamespace(dc, namespace, rewind string) error {
	cmd := fmt.Sprintf(cmdCreateXDRNamespace, dc, namespace, rewind)

	resp, err := ic.GetInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to create xdr namesapce: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse create xdr namesapce response: %w", err)
	}

	return nil
}

func (ic *InfoClient) removeXDRNamespace(dc, namespace string) error {
	cmd := fmt.Sprintf(cmdRemoveXDRNamespace, dc, namespace)

	resp, err := ic.GetInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to remove xdr namespace: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse remove xdr namespace response: %w", err)
	}

	return nil
}

func (ic *InfoClient) removeXDRNode(dc, hostPort string) error {
	cmd := fmt.Sprintf(cmdRemoveXDRNode, dc, hostPort)

	resp, err := ic.GetInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to remove xdr node: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse remove xdr node response: %w", err)
	}

	return nil
}

func (ic *InfoClient) removeXDRDC(dc string) error {
	cmd := fmt.Sprintf(cmdRemoveXDRDC, dc)

	resp, err := ic.GetInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to remove xdr dc: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse remove xdr dc response: %w", err)
	}

	return nil
}

// BlockMRTWrites blocks MRT writes on cluster.
func (ic *InfoClient) BlockMRTWrites(namespace string) error {
	cmd := fmt.Sprintf(cmdBlockMRTWrites, namespace)

	resp, err := ic.GetInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to block mrt writes: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse block mrt writes response: %w", err)
	}

	return nil
}

// UnBlockMRTWrites unblocks MRT writes on cluster.
func (ic *InfoClient) UnBlockMRTWrites(namespace string) error {
	cmd := fmt.Sprintf(cmdUnBlockMRTWrites, namespace)

	resp, err := ic.GetInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to unblock mrt writes: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse unblock mrt writes response: %w", err)
	}

	return nil
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
func (ic *InfoClient) GetStats(dc, namespace string) (Stats, error) {
	cmd := fmt.Sprintf(cmdGetStats, dc, namespace)

	resp, err := ic.GetInfo(cmd)
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

// ***** Utility functions *****

func parseResultResponse(cmd string, result map[string]string) (string, error) {
	v, ok := result[cmd]
	if !ok {
		return "", fmt.Errorf("no response for command %s", cmd)
	}

	if strings.Contains(v, cmdRespErrPrefix) {
		return "", fmt.Errorf("command %s failed: %s", cmd, v)
	}

	return v, nil
}

func getSIndexes(node infoGetter, namespace string, policy *a.InfoPolicy) ([]*models.SIndex, error) {
	supportsSIndexCTX := AerospikeVersionSupportsSIndexContext
	version, err := getAerospikeVersion(node, policy)

	if err != nil {
		return nil, fmt.Errorf("failed to get aerospike version: %w", err)
	}

	getCtx := version.IsGreaterOrEqual(supportsSIndexCTX)
	cmd := buildSindexCmd(namespace, getCtx)

	response, err := node.RequestInfo(policy, cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get sindexes: %w", err)
	}

	return parseSIndexes(response[cmd])
}

func buildSindexCmd(namespace string, getCtx bool) string {
	cmd := fmt.Sprintf("sindex-list:ns=%s", namespace)

	// NOTE: getting the sindex ctx was added in Aerospike 6.1
	// so don't include this in the command at all if the server is older
	if getCtx {
		cmd += ";b64=true"
	}

	return cmd
}

func getAerospikeVersion(conn infoGetter, policy *a.InfoPolicy) (AerospikeVersion, error) {
	versionResp, err := conn.RequestInfo(policy, "build")
	if err != nil {
		return AerospikeVersion{}, err
	}

	versionStr, ok := versionResp["build"]
	if !ok {
		return AerospikeVersion{}, fmt.Errorf("failed to get Aerospike version, info response missing 'build' key")
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

	return si, nil
}

func getUDFs(node infoGetter, policy *a.InfoPolicy) ([]*models.UDF, error) {
	cmd := "udf-list"

	response, aerr := node.RequestInfo(policy, cmd)
	if aerr != nil {
		return nil, fmt.Errorf("failed to list UDFs: %w", aerr)
	}

	udfList, err := parseUDFListResponse(response[cmd])
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

		udf, err := getUDF(node, name, policy)
		if err != nil {
			return nil, fmt.Errorf("failed to get UDF: %w", err)
		}

		udfs[i] = udf
	}

	return udfs, nil
}

func getUDF(node infoGetter, name string, policy *a.InfoPolicy) (*models.UDF, error) {
	cmd := fmt.Sprintf("udf-get:filename=%s", name)

	response, aerr := node.RequestInfo(policy, cmd)
	if aerr != nil {
		return nil, fmt.Errorf("udf-get info command failed: %w", aerr)
	}

	udfInfo, ok := response[cmd]
	if !ok {
		return nil, fmt.Errorf("command %s info response missing", cmd)
	}

	udf, err := parseUDFResponse(udfInfo)
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

func getRecordCountForNode(node infoGetter, policy *a.InfoPolicy, namespace string, sets []string) (uint64, error) {
	cmd := fmt.Sprintf("sets/%s", namespace)

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

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

func getEffectiveReplicationFactor(node infoGetter, policy *a.InfoPolicy, namespace string) (int, error) {
	cmd := fmt.Sprintf("namespace/%s", namespace)

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
