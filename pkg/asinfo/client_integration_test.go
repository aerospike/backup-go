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

//go:build integration

package asinfo

import (
	"fmt"
	"log/slog"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/require"
)

const (
	testASLoginPassword = "admin"
	testASNamespace     = "test"
	testASDC            = "DC1"
	testASHost          = "127.0.0.1"
	testASPort          = 3000
	testSetInfo         = "info_set"
)

func newAerospikeClient() (*a.Client, a.Error) {
	asPolicy := a.NewClientPolicy()
	asPolicy.User = testASLoginPassword
	asPolicy.Password = testASLoginPassword

	return a.NewClientWithPolicy(asPolicy, testASHost, testASPort)
}

func TestClient_GetSIndexes(t *testing.T) {
	t.Parallel()

	client, aerr := newAerospikeClient()
	require.NoError(t, aerr)

	ic, err := NewClient(client.Cluster(), a.NewInfoPolicy(), models.NewDefaultRetryPolicy(), slog.Default())
	require.NoError(t, err)

	ctx := t.Context()

	_, err = ic.GetSIndexes(ctx, testASNamespace)
	require.NoError(t, err)
}

func TestClient_GetUDFs(t *testing.T) {
	t.Parallel()

	client, aerr := newAerospikeClient()
	require.NoError(t, aerr)

	ic, err := NewClient(client.Cluster(), a.NewInfoPolicy(), models.NewDefaultRetryPolicy(), slog.Default())
	require.NoError(t, err)

	ctx := t.Context()

	_, err = ic.GetUDFs(ctx)
	require.NoError(t, err)
}

func TestClient_GetRecordCount(t *testing.T) {
	t.Parallel()

	client, aerr := newAerospikeClient()
	require.NoError(t, aerr)

	ic, err := NewClient(client.Cluster(), a.NewInfoPolicy(), models.NewDefaultRetryPolicy(), slog.Default())
	require.NoError(t, err)

	ctx := t.Context()

	_, err = ic.GetRecordCount(ctx, testASNamespace, nil)
	require.NoError(t, err)
}

func TestClient_GetSets(t *testing.T) {
	t.Parallel()

	client, aerr := newAerospikeClient()
	require.NoError(t, aerr)

	wp := a.NewWritePolicy(0, 0)
	k, aerr := a.NewKey(testASNamespace, testSetInfo, "get-sets")
	require.NoError(t, aerr)
	b := a.NewBin("bin", "value")
	aerr = client.PutBins(wp, k, b)
	require.NoError(t, aerr)

	ic, err := NewClient(client.Cluster(), a.NewInfoPolicy(), models.NewDefaultRetryPolicy(), slog.Default())
	require.NoError(t, err)

	ctx := t.Context()

	result, err := ic.GetSetsList(ctx, testASNamespace)
	require.NoError(t, err)

	require.Greater(t, len(result), 1)
}

func TestClient_getRackNodes(t *testing.T) {
	t.Parallel()

	client, aerr := newAerospikeClient()
	require.NoError(t, aerr)

	ic, err := NewClient(client.Cluster(), a.NewInfoPolicy(), models.NewDefaultRetryPolicy(), slog.Default())
	require.NoError(t, err)

	ctx := t.Context()

	res, err := ic.GetRackNodes(ctx, 0)
	require.NoError(t, err)

	fmt.Println(res)
}

func TestClient_getService(t *testing.T) {
	t.Parallel()

	client, aerr := newAerospikeClient()
	require.NoError(t, aerr)

	ic, err := NewClient(client.Cluster(), a.NewInfoPolicy(), models.NewDefaultRetryPolicy(), slog.Default())
	require.NoError(t, err)

	nodes := ic.GetNodesNames()

	_, err = ic.getByNode(nodes[0], cmdServiceTLSStd)
	require.NoError(t, err)
}

func TestClient_GetNamespacesList(t *testing.T) {
	t.Parallel()

	client, aerr := newAerospikeClient()
	require.NoError(t, aerr)

	ic, err := NewClient(client.Cluster(), a.NewInfoPolicy(), models.NewDefaultRetryPolicy(), slog.Default())
	require.NoError(t, err)

	ctx := t.Context()

	result, err := ic.GetNamespacesList(ctx)
	require.NoError(t, err)

	require.Equal(t, []string{testASNamespace}, result)
}

func TestClient_GetStatus(t *testing.T) {
	t.Parallel()

	client, aerr := newAerospikeClient()
	require.NoError(t, aerr)

	ic, err := NewClient(client.Cluster(), a.NewInfoPolicy(), models.NewDefaultRetryPolicy(), slog.Default())
	require.NoError(t, err)

	ctx := t.Context()

	result, err := ic.GetStatus(ctx)
	require.NoError(t, err)

	require.Equal(t, "ok", result)
}

func TestClient_GetReplicas(t *testing.T) {
	client, aerr := newAerospikeClient()
	require.NoError(t, aerr)

	ic, err := NewClient(client.Cluster(), a.NewInfoPolicy(), models.NewDefaultRetryPolicy(), slog.Default())
	require.NoError(t, err)

	node, err := ic.cluster.GetRandomNode()
	require.NoError(t, err)

	ctx := t.Context()

	b, err := ic.GetPrimaryPartitions(ctx, node.GetName(), testASNamespace)
	require.NoError(t, err)
	require.NotNil(t, b)
}

func TestClient_GetPendingMigrations(t *testing.T) {
	client, aerr := newAerospikeClient()
	require.NoError(t, aerr)

	ic, err := NewClient(client.Cluster(), a.NewInfoPolicy(), models.NewDefaultRetryPolicy(), slog.Default())
	require.NoError(t, err)

	ctx := t.Context()

	result, err := ic.GetPendingMigrations(ctx, testASNamespace)
	require.NoError(t, err)
	require.Equal(t, uint64(0), result)
}

func TestClient_getClusterStable(t *testing.T) {
	t.Parallel()

	client, aerr := newAerospikeClient()
	require.NoError(t, aerr)

	ic, err := NewClient(client.Cluster(), a.NewInfoPolicy(), models.NewDefaultRetryPolicy(), slog.Default())
	require.NoError(t, err)

	ctx := t.Context()

	_, err = ic.getClusterStable(ctx, testASNamespace)
	require.NoError(t, err)
}

func TestClient_getStatistics(t *testing.T) {
	t.Parallel()

	client, aerr := newAerospikeClient()
	require.NoError(t, aerr)

	ic, err := NewClient(client.Cluster(), a.NewInfoPolicy(), models.NewDefaultRetryPolicy(), slog.Default())
	require.NoError(t, err)

	ctx := t.Context()

	_, err = ic.getStatistics(ctx)
	require.NoError(t, err)
}

func TestClient_getPrincipal(t *testing.T) {
	t.Parallel()

	client, aerr := newAerospikeClient()
	require.NoError(t, aerr)

	ic, err := NewClient(client.Cluster(), a.NewInfoPolicy(), models.NewDefaultRetryPolicy(), slog.Default())
	require.NoError(t, err)

	ctx := t.Context()

	_, err = ic.getPrincipal(ctx)
	require.NoError(t, err)
}
