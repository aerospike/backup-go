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

package storage

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
	appConfig "github.com/aerospike/backup-go/cmd/internal/config"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/tools-common-go/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	cp := &models.ClientPolicy{
		Timeout:      1000,
		IdleTimeout:  1000,
		LoginTimeout: 1000,
	}
	_, err := NewAerospikeClient(cfg, cp, "1", 10, slog.Default())
	require.NoError(t, err)

	cfg = &client.AerospikeConfig{
		User:     testASLoginPassword,
		Password: testASLoginPassword,
	}
	_, err = NewAerospikeClient(cfg, cp, "", 10, slog.Default())
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
	_, err = NewAerospikeClient(cfg, cp, "", 10, slog.Default())
	require.ErrorContains(t, err, "failed to create Aerospike client policy")

	hostPort.Host = "255.255.255.255"
	cfg = &client.AerospikeConfig{
		Seeds: client.HostTLSPortSlice{
			hostPort,
		},
		User:     testASLoginPassword,
		Password: testASLoginPassword,
	}
	_, err = NewAerospikeClient(cfg, cp, "", 10, slog.Default())
	require.ErrorContains(t, err, "failed to create Aerospike client")
}

func TestClients_newS3Client(t *testing.T) {
	t.Parallel()
	err := createAwsCredentials()
	assert.NoError(t, err)

	cfg := &models.AwsS3{
		Region:   testS3Region,
		Profile:  testS3Profile,
		Endpoint: testS3Endpoint,
	}

	ctx := context.Background()
	_, err = newS3Client(ctx, cfg)
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

func TestToHosts(t *testing.T) {
	tests := []struct {
		name     string
		input    client.HostTLSPortSlice
		expected []*aerospike.Host
	}{
		{
			name: "Single Host",
			input: client.HostTLSPortSlice{
				{Host: "localhost", TLSName: "tls1", Port: 3000},
			},
			expected: []*aerospike.Host{
				{Name: "localhost", TLSName: "tls1", Port: 3000},
			},
		},
		{
			name: "Multiple Hosts",
			input: client.HostTLSPortSlice{
				{Host: "host1", TLSName: "tls1", Port: 3000},
				{Host: "host2", TLSName: "tls2", Port: 3001},
			},
			expected: []*aerospike.Host{
				{Name: "host1", TLSName: "tls1", Port: 3000},
				{Name: "host2", TLSName: "tls2", Port: 3001},
			},
		},
		{
			name:     "Empty Input",
			input:    client.HostTLSPortSlice{},
			expected: []*aerospike.Host{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toHosts(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseRacks(t *testing.T) {
	tests := []struct {
		name        string
		racks       string
		expected    []int
		expectError bool
		errorText   string
	}{
		{
			name:     "Single Valid Rack",
			racks:    "1",
			expected: []int{1},
		},
		{
			name:     "Multiple Valid Racks",
			racks:    "1,2,3",
			expected: []int{1, 2, 3},
		},
		{
			name:        "Invalid Rack - Non-integer",
			racks:       "1,abc,3",
			expected:    nil,
			expectError: true,
			errorText:   "failed to parse racks",
		},
		{
			name:        "Invalid Rack - Negative Value",
			racks:       "1,-2,3",
			expected:    nil,
			expectError: true,
			errorText:   "rack id -2 invalid, should be non-negative number",
		},
		{
			name:        "Invalid Rack - Exceeds MaxRack",
			racks:       fmt.Sprintf("1,%d,3", appConfig.MaxRack+1),
			expected:    nil,
			expectError: true,
			errorText: fmt.Sprintf("rack id %d invalid, should not exceed %d",
				appConfig.MaxRack+1, appConfig.MaxRack),
		},
		{
			name:     "Empty Input",
			racks:    "",
			expected: []int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := appConfig.ParseRacks(tt.racks)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
				assert.Contains(t, err.Error(), tt.errorText)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
