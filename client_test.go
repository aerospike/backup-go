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

package backup

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/sync/semaphore"
)

func TestNilClient(t *testing.T) {
	_, err := NewClient(nil)
	assert.Error(t, err, "aerospike client is required")
}

func TestClientOptions(t *testing.T) {
	var logBuffer strings.Builder
	logger := slog.New(slog.NewTextHandler(&logBuffer, nil))
	sem := semaphore.NewWeighted(10)
	id := "ID"

	client, err := NewClient(&mocks.MockAerospikeClient{},
		WithID(id),
		WithLogger(logger),
		WithScanLimiter(sem),
	)

	assert.NoError(t, err)
	assert.Equal(t, id, client.id)
	assert.Equal(t, sem, client.scanLimiter)

	client.logger.Info("test")
	assert.Contains(t, logBuffer.String(), "level=INFO msg=test backup.client.id=ID")
}

// Negative test cases for Backup method
func TestBackupNilConfig(t *testing.T) {
	mockAerospikeClient := &mocks.MockAerospikeClient{}
	client, err := NewClient(mockAerospikeClient)
	assert.NoError(t, err)

	_, err = client.Backup(context.Background(), nil, &mocks.MockWriter{}, &mocks.MockStreamingReader{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "backup config required")
}

func TestBackupNilWriter(t *testing.T) {
	mockAerospikeClient := &mocks.MockAerospikeClient{}
	mockAerospikeClient.On("GetDefaultInfoPolicy").Return(&a.InfoPolicy{})
	mockAerospikeClient.On("GetDefaultScanPolicy").Return(&a.ScanPolicy{})

	client, err := NewClient(mockAerospikeClient)
	assert.NoError(t, err)

	config := &ConfigBackup{
		Namespace: "test",
	}

	_, err = client.Backup(context.Background(), config, nil, &mocks.MockStreamingReader{})
	assert.Error(t, err)
	// The validation happens before the check for nil writer
	assert.Contains(t, err.Error(), "failed to validate backup config")
}

func TestBackupInvalidConfig(t *testing.T) {
	mockAerospikeClient := &mocks.MockAerospikeClient{}
	mockAerospikeClient.On("GetDefaultInfoPolicy").Return(&a.InfoPolicy{})
	mockAerospikeClient.On("GetDefaultScanPolicy").Return(&a.ScanPolicy{})

	client, err := NewClient(mockAerospikeClient)
	assert.NoError(t, err)

	// Create an invalid config (missing namespace)
	config := &ConfigBackup{}

	_, err = client.Backup(context.Background(), config, &mocks.MockWriter{}, &mocks.MockStreamingReader{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to validate backup config")
}

func TestBackupInvalidParallelRead(t *testing.T) {
	mockAerospikeClient := &mocks.MockAerospikeClient{}
	mockAerospikeClient.On("GetDefaultInfoPolicy").Return(&a.InfoPolicy{})
	mockAerospikeClient.On("GetDefaultScanPolicy").Return(&a.ScanPolicy{})

	client, err := NewClient(mockAerospikeClient)
	assert.NoError(t, err)

	// Create an invalid config with invalid ParallelRead
	config := &ConfigBackup{
		Namespace:     "test",
		ParallelRead:  0, // Invalid value
		ParallelWrite: 1, // Valid value
	}

	_, err = client.Backup(context.Background(), config, &mocks.MockWriter{}, &mocks.MockStreamingReader{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to validate backup config")
}

// Negative test cases for BackupXDR method
func TestBackupXDRNilConfig(t *testing.T) {
	mockAerospikeClient := &mocks.MockAerospikeClient{}
	client, err := NewClient(mockAerospikeClient)
	assert.NoError(t, err)

	_, err = client.BackupXDR(context.Background(), nil, &mocks.MockWriter{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "xdr backup config required")
}

func TestBackupXDRNilWriter(t *testing.T) {
	mockAerospikeClient := &mocks.MockAerospikeClient{}
	mockAerospikeClient.On("GetDefaultInfoPolicy").Return(&a.InfoPolicy{})

	client, err := NewClient(mockAerospikeClient)
	assert.NoError(t, err)

	config := &ConfigBackupXDR{
		DC: "test",
	}

	_, err = client.BackupXDR(context.Background(), config, nil)
	assert.Error(t, err)
	// The validation happens before the check for nil writer
	assert.Contains(t, err.Error(), "failed to validate xdr backup config")
}

func TestBackupXDRInvalidConfig(t *testing.T) {
	mockAerospikeClient := &mocks.MockAerospikeClient{}
	mockAerospikeClient.On("GetDefaultInfoPolicy").Return(&a.InfoPolicy{})

	client, err := NewClient(mockAerospikeClient)
	assert.NoError(t, err)

	// Create an invalid config (missing DC name)
	config := &ConfigBackupXDR{}

	_, err = client.BackupXDR(context.Background(), config, &mocks.MockWriter{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to validate xdr backup config")
}

// Negative test cases for Restore method
func TestRestoreNilConfig(t *testing.T) {
	mockAerospikeClient := &mocks.MockAerospikeClient{}
	client, err := NewClient(mockAerospikeClient)
	assert.NoError(t, err)

	_, err = client.Restore(context.Background(), nil, &mocks.MockStreamingReader{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "restore config required")
}

func TestRestoreNilStreamingReader(t *testing.T) {
	mockAerospikeClient := &mocks.MockAerospikeClient{}
	mockAerospikeClient.On("GetDefaultInfoPolicy").Return(&a.InfoPolicy{})
	mockAerospikeClient.On("GetDefaultWritePolicy").Return(&a.WritePolicy{})

	client, err := NewClient(mockAerospikeClient)
	assert.NoError(t, err)

	config := &ConfigRestore{
		EncoderType: EncoderTypeASB,
	}

	_, err = client.Restore(context.Background(), config, nil)
	assert.Error(t, err)
	// The validation happens before the check for nil streaming reader
	assert.Contains(t, err.Error(), "failed to validate restore config")
}

func TestRestoreInvalidConfig(t *testing.T) {
	mockAerospikeClient := &mocks.MockAerospikeClient{}
	mockAerospikeClient.On("GetDefaultInfoPolicy").Return(&a.InfoPolicy{})
	mockAerospikeClient.On("GetDefaultWritePolicy").Return(&a.WritePolicy{})

	client, err := NewClient(mockAerospikeClient)
	assert.NoError(t, err)

	// Create an invalid config (invalid encoder type)
	config := &ConfigRestore{
		EncoderType: 999, // Invalid encoder type
	}

	_, err = client.Restore(context.Background(), config, &mocks.MockStreamingReader{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to validate restore config")
}

func TestRestoreInvalidEncoderType(t *testing.T) {
	mockAerospikeClient := &mocks.MockAerospikeClient{}
	mockAerospikeClient.On("GetDefaultInfoPolicy").Return(&a.InfoPolicy{})
	mockAerospikeClient.On("GetDefaultWritePolicy").Return(&a.WritePolicy{})

	client, err := NewClient(mockAerospikeClient)
	assert.NoError(t, err)

	// Create a config with an invalid encoder type
	config := &ConfigRestore{
		EncoderType: 999, // Invalid encoder type
	}

	_, err = client.Restore(context.Background(), config, &mocks.MockStreamingReader{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to validate restore config")
}

func TestRestoreASBXValidationError(t *testing.T) {
	mockAerospikeClient := &mocks.MockAerospikeClient{}
	mockAerospikeClient.On("GetDefaultInfoPolicy").Return(&a.InfoPolicy{})
	mockAerospikeClient.On("GetDefaultWritePolicy").Return(&a.WritePolicy{})

	client, err := NewClient(mockAerospikeClient)
	assert.NoError(t, err)

	// Create a config with ASBX encoder type but missing required fields
	config := &ConfigRestore{
		EncoderType: EncoderTypeASBX,
		// Missing required fields for ASBX
	}

	_, err = client.Restore(context.Background(), config, &mocks.MockStreamingReader{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to validate restore config")
}

// Negative test cases for Estimate method
func TestEstimateNilConfig(t *testing.T) {
	mockAerospikeClient := &mocks.MockAerospikeClient{}
	client, err := NewClient(mockAerospikeClient)
	assert.NoError(t, err)

	_, err = client.Estimate(context.Background(), nil, 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "backup config required")
}

func TestEstimateInvalidConfig(t *testing.T) {
	mockAerospikeClient := &mocks.MockAerospikeClient{}
	mockAerospikeClient.On("GetDefaultInfoPolicy").Return(&a.InfoPolicy{})
	mockAerospikeClient.On("GetDefaultScanPolicy").Return(&a.ScanPolicy{})

	client, err := NewClient(mockAerospikeClient)
	assert.NoError(t, err)

	// Create an invalid config (missing namespace)
	config := &ConfigBackup{}

	_, err = client.Estimate(context.Background(), config, 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to validate backup config")
}

func TestEstimateGetEstimateError(t *testing.T) {
	// Mock the AerospikeClient
	mockAerospikeClient := &mocks.MockAerospikeClient{}
	mockAerospikeClient.On("GetDefaultInfoPolicy").Return(&a.InfoPolicy{})
	mockAerospikeClient.On("GetDefaultScanPolicy").Return(&a.ScanPolicy{})

	// Create a mock cluster
	mockCluster := &a.Cluster{}
	mockAerospikeClient.On("Cluster").Return(mockCluster)

	// Mock a node
	mockNode := &a.Node{}
	mockAerospikeClient.On("GetNodes").Return([]*a.Node{mockNode})

	// Create a client with the mock
	client, err := NewClient(mockAerospikeClient)
	assert.NoError(t, err)

	// Create a valid config
	config := &ConfigBackup{
		Namespace:     "test",
		ParallelRead:  1, // Set a valid value to pass validation
		ParallelWrite: 1, // Set a valid value to pass validation
	}

	// Mock the cluster info response to cause an error in getEstimate
	mockAerospikeClient.On("ScanNode", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, a.ErrInvalidParam)

	// Call Estimate
	_, err = client.Estimate(context.Background(), config, -1) // Negative sample size
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get estimate")
}
