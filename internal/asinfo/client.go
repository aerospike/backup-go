// Copyright 2024-2024 Aerospike, Inc.
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
	"fmt"
	"regexp"
	"strconv"
	"strings"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/models"
)

var aerospikeVersionRegex = regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)`)

type AerospikeVersion struct {
	Major int
	Minor int
	Patch int
}

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
//go:generate mockery --name infoGetter
type infoGetter interface {
	RequestInfo(infoPolicy *a.InfoPolicy, commands ...string) (map[string]string, a.Error)
}

type InfoClient struct {
	node   infoGetter
	policy *a.InfoPolicy
}

func NewInfoClient(cg infoGetter, policy *a.InfoPolicy) *InfoClient {
	ic := &InfoClient{
		node:   cg,
		policy: policy,
	}

	return ic
}

func NewInfoClientFromAerospike(aeroClient *a.Client, policy *a.InfoPolicy) (*InfoClient, error) {
	node, err := aeroClient.Cluster().GetRandomNode()
	if err != nil {
		return nil, err
	}
	return NewInfoClient(node, policy), nil
}

func (ic *InfoClient) GetInfo(names ...string) (map[string]string, error) {
	return ic.node.RequestInfo(ic.policy, names...)
}

func (ic *InfoClient) GetVersion() (AerospikeVersion, error) {
	return getAerospikeVersion(ic.node, ic.policy)
}

func (ic *InfoClient) GetSIndexes(namespace string) ([]*models.SIndex, error) {
	return getSIndexes(ic.node, namespace, ic.policy)
}

// ***** Utility functions *****

var AerospikeVersionSupportsSIndexContext = AerospikeVersion{6, 1, 0}

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

	return parseSIndexResponse(response[cmd])
}

func buildSindexCmd(namespace string, getCtx bool) string {
	cmd := fmt.Sprintf("sindex-list:namespace=%s", namespace)

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

func parseSIndexResponse(sindexInfoResp string) ([]*models.SIndex, error) {
	if sindexInfoResp == "" {
		return nil, nil
	}

	// Remove the trailing semicolon if it exists
	if sindexInfoResp[len(sindexInfoResp)-1] == ';' {
		sindexInfoResp = sindexInfoResp[:len(sindexInfoResp)-1]
	}

	// No secondary indexes
	if sindexInfoResp == "" {
		return nil, nil
	}

	sindexInfo, err := parseInfoResponse(sindexInfoResp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse sindex response: %w", err)
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

// parseSindex parses a single infoMap containing a sindex into a SecondaryIndex model
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

type infoMap map[string]string

// parseInfoResponse parses a single info response format string.
// the string may contain multiple response objects each separated by a semicolon
// each key-value pair is separated by a colon and the key is separated from the value by an equals sign
// e.g. "foo=bar:baz=qux;foo=bar:baz=qux"
// the above example is returned as []infoMap{infoMap{"foo": "bar", "baz": "qux"}, infoMap{"foo": "bar", "baz": "qux"}}
func parseInfoResponse(resp string) ([]infoMap, error) {
	objects := strings.Split(resp, ";")
	info := make([]infoMap, len(objects))

	for i, object := range objects {
		data := map[string]string{}
		kvpairs := strings.Split(object, ":")

		for _, pair := range kvpairs {
			// some info key value pairse can contain "=" in the value
			// for example, the base64 encoded context for a secondary index
			// so we need to split on the first "=" only
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) != 2 {
				return nil, fmt.Errorf("invalid key-value pair: %s", pair)
			}

			data[kv[0]] = kv[1]
			info[i] = data
		}
	}

	return info, nil
}
