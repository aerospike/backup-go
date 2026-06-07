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
	"cmp"
	"context"
	"encoding/base64"
	"fmt"
	"slices"
	"strconv"
	"strings"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
	m "github.com/aerospike/backup-go/pkg/asinfo/models"
)

func parseUDFResponse(udfGetInfoResp string) (*models.UDF, error) {
	udfInfo, err := parseUDFGetResponse(udfGetInfoResp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse UDF response: %w", err)
	}

	udf, err := parseUDF(udfInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to parse UDF: %w", err)
	}

	return udf, nil
}

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
	supportsSIndexCTX := m.AerospikeVersionSupportsSIndexContext

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

func (ic *Client) getAerospikeVersion(conn infoGetter, policy *a.InfoPolicy) (m.AerospikeVersion, error) {
	// As we need to check version before we form dict, this command will be loaded directly.
	cmd := cmdBuild

	versionResp, aErr := conn.RequestInfo(policy, cmd)
	if aErr != nil {
		return m.AerospikeVersion{}, aErr
	}

	versionStr, err := parseResultResponse(cmd, versionResp)
	if err != nil {
		return m.AerospikeVersion{}, fmt.Errorf("failed to parse get version response: %s: %w", versionResp, err)
	}

	return parseAerospikeVersion(versionStr)
}

func parseAerospikeVersion(versionStr string) (m.AerospikeVersion, error) {
	matches := m.AerospikeVersionRegex.FindStringSubmatch(versionStr)
	if len(matches) != 4 {
		return m.AerospikeVersion{}, fmt.Errorf("failed to parse Aerospike version from '%s'", versionStr)
	}

	major, err := strconv.Atoi(matches[1])
	if err != nil {
		return m.AerospikeVersion{}, fmt.Errorf("failed to parse Aerospike major version %w", err)
	}

	minor, err := strconv.Atoi(matches[2])
	if err != nil {
		return m.AerospikeVersion{}, fmt.Errorf("failed to parse Aerospike minor version %w", err)
	}

	patch, err := strconv.Atoi(matches[3])
	if err != nil {
		return m.AerospikeVersion{}, fmt.Errorf("failed to parse Aerospike patch version %w", err)
	}

	return m.AerospikeVersion{
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

// parseSIndex parses a single InfoMap containing a sindex into a SecondaryIndex model
func parseSIndex(sindexMap m.InfoMap) (*models.SIndex, error) {
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
		case indexTypeDefault, indexTypeNone:
			sindexType = models.BinSIndex
		case indexTypeList:
			sindexType = models.ListElementSIndex
		case indexTypeMapKeys:
			sindexType = models.MapKeySIndex
		case indexTypeMapValues:
			sindexType = models.MapValueSIndex
		default:
			return nil, fmt.Errorf("invalid sindex index type: %s", val)
		}

		si.IndexType = sindexType
	} else {
		return nil, fmt.Errorf("sindex missing indextype")
	}

	if val, ok := sindexMap["bin"]; ok { //nolint:nestif // parsing optional map fields: bin → type → context
		path := models.SIndexPath{
			BinName: val,
		}

		if val, ok := sindexMap["type"]; ok {
			var binType models.SIPathBinType

			switch strings.ToLower(val) {
			case indexBinTypeNumeric, indexBinTypeIntSigned:
				binType = models.NumericSIDataType
			case indexBinTypeString, indexBinTypeText:
				binType = models.StringSIDataType
			case indexBinTypeBlob:
				binType = models.BlobSIDataType
			case indexBinTypeGeo2DSphere, indexBinTypeGeoJSON:
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

func parseUDF(udfMap m.InfoMap) (*models.UDF, error) {
	var (
		udf     models.UDF
		udfLang string
	)

	if val, ok := udfMap["type"]; ok {
		udfLang = val
	} else {
		return nil, fmt.Errorf("UDF info response missing language type")
	}

	if strings.EqualFold(udfLang, "lua") {
		udf.UDFType = models.UDFTypeLUA
	} else {
		return nil, fmt.Errorf("invalid UDF language type: %s", udfLang)
	}

	if val, ok := udfMap["content"]; ok {
		// the udf content field is base64 encoded in info responses
		content, err := base64.StdEncoding.DecodeString(val)
		if err != nil {
			return nil, fmt.Errorf("failed to decode UDF content: %w", err)
		}

		udf.Content = content
	} else {
		return nil, fmt.Errorf("UDF info response missing content")
	}

	return &udf, nil
}

// parseInfoResponse parses a single info response format string.
// the string may contain multiple response objects each separated by a semicolon
// each key-value pair is separated by a colon and the key is separated from the value by an equals sign
// e.g. "foo=bar:baz=qux;foo=bar:baz=qux"
// the above example is returned as []infoMap{infoMap{"foo": "bar", "baz": "qux"}, InfoMap{"foo": "bar", "baz": "qux"}}
// if the passed in info response is empty, nil is returned.
func parseInfoResponse(resp, objSep, pairSep, kvSep string) ([]m.InfoMap, error) {
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
	info := make([]m.InfoMap, len(objects))

	for i, object := range objects {
		data, err := parseInfoObject(object, pairSep, kvSep)
		if err != nil {
			return nil, err
		}

		info[i] = data
	}

	return info, nil
}

func parseInfoObject(obj, pairSep, kvSep string) (m.InfoMap, error) {
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
func parseSindexListResponse(resp string) ([]m.InfoMap, error) {
	return parseInfoResponse(resp, ";", ":", "=")
}

// parseUDFListResponse parses a udf-list info response
// example resp: filename=basic_udf.lua,hash=706c57cb29e027221560a3cb4b693573ada98bf2,type=LUA;...
func parseUDFListResponse(resp string) ([]m.InfoMap, error) {
	return parseInfoResponse(resp, ";", ",", "=")
}

// parseUDFGetResponse parses a udf-get info response
// example resp: type=LUA;content=LS0gQSB2ZXJ5IHNpbXBsZSBhcml0
func parseUDFGetResponse(resp string) (m.InfoMap, error) {
	return parseInfoObject(resp, ";", "=")
}

func executeWithRetry(ctx context.Context, policy *models.RetryPolicy, command func() error) error {
	if policy == nil {
		return fmt.Errorf("retry policy cannot be nil")
	}

	return policy.Do(ctx, command)
}

// base64StringToBitArray decodes a base64 string and converts the result to a bitarray (slice of booleans).
func base64StringToBitArray(base64Str string) ([]bool, error) {
	decodedBytes, err := base64.StdEncoding.DecodeString(base64Str)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 string: %w", err)
	}

	bitarray := make([]bool, 0, len(decodedBytes)*8) // Pre-allocate for efficiency

	for _, b := range decodedBytes {
		for i := 7; i >= 0; i-- {
			// Check if the i-th bit is set (from most significant to least significant)
			if (b>>i)&1 == 1 {
				bitarray = append(bitarray, true)
			} else {
				bitarray = append(bitarray, false)
			}
		}
	}

	return bitarray, nil
}

func bitMapToIntSlice(b []bool) []int {
	var result []int

	for i, bit := range b {
		if bit {
			result = append(result, i)
		}
	}

	return result
}

// filterBackupsSortedByTimeSinceDone filters backup jobs from the given list
// and sorts them by time-since-done in ascending order.
func filterBackupsSortedByTimeSinceDone(jobs []m.InfoMap) ([]m.InfoMap, error) {
	fJobs := make([]m.InfoMap, 0)

	for _, job := range jobs {
		if job["job-type"] != jobTypeBackup {
			continue
		}
		fJobs = append(fJobs, job)
	}

	type indexed struct {
		t       int64
		present bool
		job     m.InfoMap
	}

	idx := make([]indexed, len(fJobs))
	for i, j := range fJobs {
		v, ok, err := j.ParseInt64("time-since-done")
		if err != nil {
			return nil, fmt.Errorf("job %d: %w", i, err)
		}
		idx[i] = indexed{t: v, present: ok, job: j}
	}

	slices.SortFunc(idx, func(a, b indexed) int {
		switch {
		case !a.present && !b.present:
			return 0
		case !a.present:
			return 1
		case !b.present:
			return -1
		}

		return cmp.Compare(a.t, b.t)
	})

	for i, x := range idx {
		fJobs[i] = x.job
	}

	return fJobs, nil
}
