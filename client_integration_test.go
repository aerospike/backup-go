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

//go:build integration

package backup

import (
	"log/slog"
	"strings"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

const (
	// A state file continuation is only valid when both are set.
	testContinuePageSize  = 100
	testContinueFileLimit = 100_000
)

func TestClientOptions(t *testing.T) {
	t.Parallel()

	var logBuffer strings.Builder
	logger := slog.New(slog.NewTextHandler(&logBuffer, nil))
	sem := semaphore.NewWeighted(10)

	testAeroClient, aerr := testAerospikeClient()
	require.NoError(t, aerr)

	client, err := NewClient(
		testAeroClient,
		WithLogger(logger),
		WithScanLimiter(sem),
		WithInfoPolicies(&a.InfoPolicy{}, models.NewDefaultRetryPolicy()),
	)

	require.NoError(t, err)
	assert.Equal(t, sem, client.scanLimiter)

	client.logger.Info("test")
	assert.Contains(t, logBuffer.String(), "level=INFO msg=test")
}

// Negative test cases for Backup method
func TestBackupNilConfig(t *testing.T) {
	t.Parallel()

	testAeroClient, aerr := testAerospikeClient()
	require.NoError(t, aerr)

	client, err := NewClient(testAeroClient)
	require.NoError(t, err)

	_, err = client.Backup(t.Context(), nil, &mocks.MockWriter{}, &mocks.MockStreamingReader{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "backup config required")
}

// TestBackupIODependencies covers the IO arguments Client.Backup requires.
// Which of them are required depends on the config, so every case starts from a
// config that passes validation: an invalid one would fail earlier and mask the
// check under test.
func TestBackupIODependencies(t *testing.T) {
	t.Parallel()

	testAeroClient, aerr := testAerospikeClient()
	require.NoError(t, aerr)

	client, err := NewClient(testAeroClient)
	require.NoError(t, err)

	tests := []struct {
		name    string
		setup   func(c *ConfigBackup)
		writer  Writer
		reader  StreamingReader
		wantErr string
	}{
		{
			name:    "nil writer is rejected",
			writer:  nil,
			reader:  &mocks.MockStreamingReader{},
			wantErr: "backup writer required",
		},
		{
			name: "nil reader is rejected when continuing from a state file",
			setup: func(c *ConfigBackup) {
				c.StateFile = testStateFile
				c.Continue = true
				c.PageSize = testContinuePageSize
				c.FileLimit = testContinueFileLimit
			},
			writer:  &mocks.MockWriter{},
			reader:  nil,
			wantErr: "streaming reader required to continue backup",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := NewDefaultBackupConfig()
			if tt.setup != nil {
				tt.setup(config)
			}

			_, err := client.Backup(t.Context(), config, tt.writer, tt.reader)
			require.ErrorIs(t, err, ErrInvalidConfig)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestBackupInvalidConfig(t *testing.T) {
	t.Parallel()

	testAeroClient, aerr := testAerospikeClient()
	require.NoError(t, aerr)

	client, err := NewClient(testAeroClient)
	require.NoError(t, err)

	// Create an invalid config (missing namespace)
	config := &ConfigBackup{}

	_, err = client.Backup(t.Context(), config, &mocks.MockWriter{}, &mocks.MockStreamingReader{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to validate backup config")
}

func TestBackupInvalidParallelRead(t *testing.T) {
	t.Parallel()

	testAeroClient, aerr := testAerospikeClient()
	require.NoError(t, aerr)

	client, err := NewClient(testAeroClient)
	require.NoError(t, err)

	// Create an invalid config with invalid ParallelRead
	config := &ConfigBackup{
		Namespace:     "test",
		ParallelRead:  0, // Invalid value
		ParallelWrite: 1, // Valid value
	}

	_, err = client.Backup(t.Context(), config, &mocks.MockWriter{}, &mocks.MockStreamingReader{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to validate backup config")
}

// Negative test cases for Restore method
func TestRestoreNilConfig(t *testing.T) {
	t.Parallel()

	testAeroClient, aerr := testAerospikeClient()
	require.NoError(t, aerr)

	client, err := NewClient(testAeroClient)
	require.NoError(t, err)

	_, err = client.Restore(t.Context(), nil, &mocks.MockStreamingReader{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "restore config required")
}

func TestRestoreNilStreamingReader(t *testing.T) {
	t.Parallel()

	testAeroClient, aerr := testAerospikeClient()
	require.NoError(t, aerr)

	client, err := NewClient(testAeroClient)
	require.NoError(t, err)

	// The config must be valid, otherwise its own error masks the nil reader.
	config := NewDefaultRestoreConfig()

	_, err = client.Restore(t.Context(), config, nil)
	require.ErrorIs(t, err, ErrInvalidConfig)
	assert.ErrorContains(t, err, "restore streaming reader required")
}

func TestRestoreInvalidConfig(t *testing.T) {
	t.Parallel()

	testAeroClient, aerr := testAerospikeClient()
	require.NoError(t, aerr)

	client, err := NewClient(testAeroClient)
	require.NoError(t, err)

	config := &ConfigRestore{
		Parallel: 0,
	}

	_, err = client.Restore(t.Context(), config, &mocks.MockStreamingReader{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to validate restore config")
}

func TestRestoreInvalidParallel(t *testing.T) {
	t.Parallel()

	testAeroClient, aerr := testAerospikeClient()
	require.NoError(t, aerr)

	client, err := NewClient(testAeroClient)
	require.NoError(t, err)

	config := &ConfigRestore{
		Parallel: -1,
	}

	_, err = client.Restore(t.Context(), config, &mocks.MockStreamingReader{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to validate restore config")
}

// Negative test cases for Estimate method
func TestEstimateNilConfig(t *testing.T) {
	t.Parallel()

	testAeroClient, aerr := testAerospikeClient()
	require.NoError(t, aerr)

	client, err := NewClient(testAeroClient)
	require.NoError(t, err)

	_, err = client.Estimate(t.Context(), nil, 100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "backup config required")
}

func TestEstimateInvalidConfig(t *testing.T) {
	t.Parallel()

	testAeroClient, aerr := testAerospikeClient()
	require.NoError(t, aerr)

	client, err := NewClient(testAeroClient)
	require.NoError(t, err)

	// Create an invalid config (missing namespace)
	config := &ConfigBackup{}

	_, err = client.Estimate(t.Context(), config, 100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to validate backup config")
}

func TestEstimateGetEstimateError(t *testing.T) {
	t.Parallel()

	testAeroClient, aerr := testAerospikeClient()
	require.NoError(t, aerr)

	client, err := NewClient(testAeroClient)
	require.NoError(t, err)

	// Create a valid config
	config := NewDefaultBackupConfig()

	// Call Estimate
	_, err = client.Estimate(t.Context(), config, -1) // Negative sample size
	require.Error(t, err)
	assert.Contains(t, err.Error(), "samples records number is negative")
}
