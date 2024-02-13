package infoclient

import (
	"backuplib/models"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	a "github.com/aerospike/aerospike-client-go/v7"
)

const (
	defaultTimeout = time.Second * 2
)

type Connection interface {
	RequestInfo(names ...string) (map[string]string, error)
}

type wrappedAerospikeConnection struct {
	conn *a.Connection
}

func (w *wrappedAerospikeConnection) RequestInfo(names ...string) (map[string]string, error) {
	return w.conn.RequestInfo(names...)
}

var aerospikeVersionRegex = regexp.MustCompile(`(\d+)\.(\d+)\.(\d+)`)

type AerospikeVersion struct {
	Major int
	Minor int
	Patch int
}

func (av AerospikeVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", av.Major, av.Minor, av.Patch)
}

func (av AerospikeVersion) IsGreaterThan(other AerospikeVersion) bool {
	if av.Major <= other.Major {
		return false
	}

	if av.Minor <= other.Minor {
		return false
	}

	if av.Patch <= other.Patch {
		return false
	}

	return true
}

type InfoClientOpts struct {
	InfoTimeout time.Duration
}

func NewDefaultInfoClientOpts() *InfoClientOpts {
	return &InfoClientOpts{
		InfoTimeout: defaultTimeout,
	}
}

type InfoClient struct {
	conn Connection
	opts *InfoClientOpts
}

func NewInfoClient(conn Connection, opts *InfoClientOpts) *InfoClient {
	ic := &InfoClient{
		conn: conn,
	}

	if opts == nil {
		opts = NewDefaultInfoClientOpts()
	}

	return ic
}

func NewInfoClientFromAerospike(aeroClient *a.Client, opts *InfoClientOpts) (*InfoClient, error) {
	conn, err := aeroClient.GetNodes()[0].GetConnection(opts.InfoTimeout)
	if err != nil {
		return nil, err
	}

	wrappedClient := &wrappedAerospikeConnection{conn: conn}

	return NewInfoClient(wrappedClient, opts), nil
}

func (ic *InfoClient) GetInfo(names ...string) (map[string]string, error) {
	return ic.conn.RequestInfo(names...)
}

func (ic *InfoClient) GetVersion() (AerospikeVersion, error) {
	return getAerospikeVersion(ic.conn)
}

func (ic *InfoClient) GetSIndexes(namespace string) ([]*models.SecondaryIndex, error) {
	return getSIndexes(ic.conn, namespace)
}

// ***** Utility functions *****

func getSIndexes(conn Connection, namespace string) ([]*models.SecondaryIndex, error) {
	supportsSIndexCTX := AerospikeVersion{6, 1, 0}
	version, err := getAerospikeVersion(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to get aerospike version: %w", err)
	}

	getCtx := version.IsGreaterThan(supportsSIndexCTX) || version == supportsSIndexCTX
	cmd := buildSindexCmd(namespace, getCtx)

	response, err := conn.RequestInfo(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get sindexes: %w", err)
	}

	return parseSIndexResponse(response[cmd])
}

func buildSindexCmd(namespace string, getCtx bool) string {
	cmd := fmt.Sprintf("sindex-list:namespace=%s", namespace)

	// NOTE: getting the sindex ctx was added in Aerospike 6.1
	// so don't include this in the command at all if the server is older
	// TODO only add this if server is 6.1 or newer
	if getCtx {
		cmd = cmd + ";get-ctx=true"
	}

	return cmd
}

func getAerospikeVersion(conn Connection) (AerospikeVersion, error) {
	versionResp, err := conn.RequestInfo("build")
	if err != nil {
		return AerospikeVersion{}, err
	}

	versionStr := versionResp["build"]

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

func parseSIndexResponse(sindexString string) ([]*models.SecondaryIndex, error) {
	if sindexString == "" {
		return nil, nil
	}

	// Remove the trailing semicolon if it exists
	if sindexString[len(sindexString)-1] == ';' {
		sindexString = sindexString[:len(sindexString)-1]
	}

	// No secondary indexes
	if sindexString == "" {
		return nil, nil
	}

	sindexInfo, err := parseInfoResponse(sindexString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse sindex response: %w", err)
	}

	sindexes := make([]*models.SecondaryIndex, len(sindexInfo))
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
func parseSIndex(sindexMap infoMap) (*models.SecondaryIndex, error) {
	si := &models.SecondaryIndex{}

	if val, ok := sindexMap["ns"]; ok {
		si.Namespace = val
	} else {
		return nil, fmt.Errorf("sindex missing namespace")
	}

	if val, ok := sindexMap["set"]; ok {
		si.Set = val
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
		}

		if val, ok := sindexMap["context"]; ok {
			path.B64Context = val
		}

		si.Path = path
	} else {
		return nil, fmt.Errorf("sindex missing bin")
	}

	return si, nil
}

type infoMap map[string]string

// parseInfoResponse parses a single info response format string.
// the string may contain multiple reponse objects each seperated by a semicolon
// each key-value pair is seperated by a colon and the key is seperated from the value by an equals sign
// e.g. "obj1=foo:bar;obj2=baz:qux"
// the above example is returned as []infoMap{infoMap{"foo": "bar"}, infoMap{"baz": "qux"}}
func parseInfoResponse(resp string) ([]infoMap, error) {
	objects := strings.Split(resp, ";")
	info := make([]infoMap, len(objects))
	for i, object := range objects {
		data := map[string]string{}
		kvpairs := strings.Split(object, ":")
		for _, pair := range kvpairs {
			kv := strings.Split(pair, "=")
			if len(kv) != 2 {
				return nil, fmt.Errorf("invalid key-value pair: %s", pair)
			}
			data[kv[0]] = kv[1]
			info[i] = data
		}
	}
	return info, nil
}
