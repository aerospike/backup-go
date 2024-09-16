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

package app

import (
	"testing"

	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/tools-common-go/client"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

const testASLoginPassword = "admin"

func TestClients_newAerospikeClient(t *testing.T) {
	t.Parallel()

	hostPort := client.NewDefaultHostTLSPort()
	cfg := &client.AerospikeConfig{
		Seeds: client.HostTLSPortSlice{
			hostPort,
		},
		User:     testASLoginPassword,
		Password: testASLoginPassword,
	}
	_, err := newAerospikeClient(cfg)
	require.NoError(t, err)

	cfg = &client.AerospikeConfig{
		User:     testASLoginPassword,
		Password: testASLoginPassword,
	}
	_, err = newAerospikeClient(cfg)
	require.ErrorContains(t, err, "at least one seed must be provided")

	cfg = &client.AerospikeConfig{
		Seeds: client.HostTLSPortSlice{
			hostPort,
		},
		User:     testASLoginPassword,
		Password: testASLoginPassword,
		TLS: &client.TLSConfig{
			Cert: []byte("error"),
		},
	}
	_, err = newAerospikeClient(cfg)
	require.ErrorContains(t, err, "failed to create Aerospike client policy")

	hostPort.Host = "255.255.255.255"
	cfg = &client.AerospikeConfig{
		Seeds: client.HostTLSPortSlice{
			hostPort,
		},
		User:     testASLoginPassword,
		Password: testASLoginPassword,
	}
	_, err = newAerospikeClient(cfg)
	require.ErrorContains(t, err, "failed to create Aerospike client")
}

func TestClients_newS3Client(t *testing.T) {
	t.Parallel()

	cfg := &models.AwsS3{
		Region:   testS3Region,
		Profile:  testS3Profile,
		Endpoint: testS3Endpoint,
	}

	ctx := context.Background()
	_, err := newS3Client(ctx, cfg)
	require.NoError(t, err)
}

func TestClients_newGcpClient(t *testing.T) {
	t.Parallel()

	cfg := &models.GcpStorage{
		Endpoint: testGcpEndpoint,
	}

	ctx := context.Background()
	_, err := newGcpClient(ctx, cfg)
	require.NoError(t, err)
}

func TestClients_newAzureClient(t *testing.T) {
	t.Parallel()

	cfg := &models.AzureBlob{
		AccountName:   testAzureAccountName,
		AccountKey:    testAzureAccountKey,
		Endpoint:      testAzureEndpoint,
		ContainerName: testBucket,
	}

	_, err := newAzureClient(cfg)
	require.NoError(t, err)
}
