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
	"log/slog"
	"regexp"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/errclass"
	"github.com/aerospike/backup-go/models"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
)

const errCmdRespPrefix = "ERROR"

const (
	indexTypeDefault   = "default"
	indexTypeNone      = "none"
	indexTypeList      = "list"
	indexTypeMapKeys   = "mapkeys"
	indexTypeMapValues = "mapvalues"
	indexTypeSet       = "set"

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
	// minReplicasFields is the minimum number of comma-separated fields expected
	// in a single namespace entry of the "replicas" info response.
	minReplicasFields = 3
)

var (
	ErrReplicationFactorZero = fmt.Errorf("%w: replication factor is zero", errclass.ErrAerospike)
	ErrNoNode                = fmt.Errorf("%w: no node found", errclass.ErrNotFound)
	ErrInvalidSIndexType     = fmt.Errorf("%w: invalid sindex index type", errclass.ErrCorruptData)

	// ErrNotFound is returned when the cluster holds no state for the requested
	// job or namespace. It is a distinct sentinel in the [errclass.ErrNotFound]
	// class, not the class itself, so matching it stays specific to this package.
	ErrNotFound = fmt.Errorf("%w: info not found", errclass.ErrNotFound)

	// Static internal errors. Kept as package-level sentinels so they can be
	// matched with errors.Is and satisfy err113/perfsprint linters.
	errNoInfoCommands = fmt.Errorf("%w: no info commands provided or command not supported",
		errclass.ErrInvalidConfig)
	errNoNodesAvailable          = fmt.Errorf("%w: no nodes available in cluster", errclass.ErrAerospike)
	errNoNodesConnected          = fmt.Errorf("%w: no nodes connected", errclass.ErrAerospike)
	errReplicationFactorNotFound = fmt.Errorf("%w: replication factor not found", errclass.ErrAerospike)
	errParseRecordInfo           = fmt.Errorf("%w: failed to parse record info request", errclass.ErrAerospike)
	errUDFMissingFilename        = fmt.Errorf("%w: udf-list response missing filename", errclass.ErrAerospike)

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
	logger      *slog.Logger
}

// NewClient initializes and returns a new asinfo Client instance with the provided Aerospike client,
// policy, and retry policy.
func NewClient(
	cluster NodeGetter,
	policy *a.InfoPolicy,
	retryPolicy *models.RetryPolicy,
	logger *slog.Logger,
) (*Client, error) {
	if retryPolicy == nil {
		retryPolicy = models.NewDefaultRetryPolicy()
	}

	ic := &Client{
		cluster:     cluster,
		policy:      policy,
		retryPolicy: retryPolicy,
		logger:      logger,
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
//
// It must not be called from inside an already retried operation: use [Client.getInfo]
// there, so that the retry counts of the two levels are not multiplied.
func (ic *Client) GetInfo(ctx context.Context, names ...string) (map[string]string, error) {
	// The commands are checked before the retry loop, because an unsupported
	// command will not become supported on the next attempt.
	if err := validateInfoCommands(names); err != nil {
		return nil, err
	}

	return retryValue(ctx, ic.retryPolicy, func() (map[string]string, error) {
		return ic.getInfo(names...)
	})
}

// getInfo runs the given info commands against a random cluster node once, without
// retrying. Callers that are themselves retried use it instead of [Client.GetInfo].
func (ic *Client) getInfo(names ...string) (map[string]string, error) {
	if err := validateInfoCommands(names); err != nil {
		return nil, err
	}

	// The class is attached here, inside the retried command, so that a context
	// error returned by the retry policy itself is not reported as a cluster
	// failure.
	node, err := ic.cluster.GetRandomNode()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errclass.ErrAerospike, err)
	}

	result, err := node.RequestInfo(ic.policy, names...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errclass.ErrAerospike, err)
	}

	return result, nil
}

// validateInfoCommands reports whether any info command was provided. An empty
// command means the command dictionary holds no entry for the server version.
func validateInfoCommands(names []string) error {
	if len(names) == 0 || names[0] == "" {
		return errNoInfoCommands
	}

	return nil
}

// requestByNode sends an info command to the specified node
// and returns the parsed response or an error if it fails.
func (ic *Client) requestByNode(node infoGetter, cmd string) (string, error) {
	resp, aErr := node.RequestInfo(ic.policy, cmd)
	if aErr != nil {
		return "", fmt.Errorf("%w: %w", errclass.ErrAerospike, aErr)
	}

	return parseCmdResponse(cmd, resp)
}

// requestRandomNode sends an info command to a random cluster node with retries
// and returns the parsed response or an error if it fails.
func (ic *Client) requestRandomNode(ctx context.Context, cmd string) (string, error) {
	resp, err := ic.GetInfo(ctx, cmd)
	if err != nil {
		return "", err
	}

	return parseCmdResponse(cmd, resp)
}

// requestRandomNodeOnce is [Client.requestRandomNode] without retrying. Callers
// that are themselves retried use it instead of [Client.requestRandomNode].
func (ic *Client) requestRandomNodeOnce(cmd string) (string, error) {
	resp, err := ic.getInfo(cmd)
	if err != nil {
		return "", err
	}

	return parseCmdResponse(cmd, resp)
}

// parseCmdResponse picks the response of cmd out of an info response.
func parseCmdResponse(cmd string, resp map[string]string) (string, error) {
	result, err := parseResultResponse(cmd, resp)
	if err != nil {
		return "", fmt.Errorf("failed to parse %s info response: %w", redactCmd(cmd), err)
	}

	return result, nil
}

// requestByNodeName sends an info command to the specified node by name
// and returns the parsed response or an error if it fails.
func (ic *Client) requestByNodeName(nodeName, cmd string) (string, error) {
	node, aErr := ic.cluster.GetNodeByName(nodeName)
	if aErr != nil {
		return "", fmt.Errorf("%w: failed to get node %s for command %s: %w",
			errclass.ErrAerospike, nodeName, redactCmd(cmd), aErr)
	}

	return ic.requestByNode(node, cmd)
}

// sendToPrincipal runs an info command against the cluster principal node and
// discards its response. Every caller runs inside a retried operation, so no
// retrying is done here.
func (ic *Client) sendToPrincipal(cmd string) error {
	principal, err := ic.getPrincipalName()
	if err != nil {
		return fmt.Errorf("failed to get cluster principal: %w", err)
	}

	_, err = ic.requestByNodeName(principal, cmd)

	return err
}

// getPrincipalNode returns the infoGetter of the cluster principal node.
func (ic *Client) getPrincipalNode() (infoGetter, error) {
	name, err := ic.getPrincipalName()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster principal name: %w", err)
	}

	node, err := ic.cluster.GetNodeByName(name)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster principal node: %w", err)
	}

	return node, nil
}

// getPrincipalName returns the name of the cluster principal node. Every caller runs
// inside a retried operation, so no retrying is done here.
func (ic *Client) getPrincipalName() (string, error) {
	stats, err := ic.getStatistics()
	if err != nil {
		return "", fmt.Errorf("failed to get cluster statistics: %w", err)
	}

	principal, ok := searchInInfoResponse(stats, "cluster_principal")
	if !ok {
		return "", fmt.Errorf("%w: cluster key not found in statistics", errclass.ErrAerospike)
	}

	if principal == "" {
		return "", fmt.Errorf("%w: cluster principal is empty", errclass.ErrAerospike)
	}

	return principal, nil
}

func searchInInfoResponse(infoResponse []infomodels.InfoMap, key string) (string, bool) {
	for _, r := range infoResponse {
		val, ok := r[key]
		if ok {
			return val, true
		}
	}

	return "", false
}
