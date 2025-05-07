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

package restore

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	appBackup "github.com/aerospike/backup-go/cmd/internal/backup"
	config2 "github.com/aerospike/backup-go/cmd/internal/config"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/backup-go/cmd/internal/storage"
	"github.com/aerospike/tools-common-go/client"
	"github.com/stretchr/testify/require"
)

const (
	testNamespace       = "test"
	testSet             = "test"
	testStateFile       = "state"
	testASLoginPassword = "admin"
)

// Test_BackupRestore one test for both so we can restore from just backed-up files.
func Test_BackupRestore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	hostPort := client.NewDefaultHostTLSPort()

	asbParams := &config2.BackupParams{
		App: &models.App{},
		ClientConfig: &client.AerospikeConfig{
			Seeds: client.HostTLSPortSlice{
				hostPort,
			},
			User:     testASLoginPassword,
			Password: testASLoginPassword,
		},
		ClientPolicy: &models.ClientPolicy{
			Timeout:      1000,
			IdleTimeout:  1000,
			LoginTimeout: 1000,
		},
		Backup: &models.Backup{
			InfoMaxRetries:                3,
			InfoRetriesMultiplier:         1,
			InfoRetryIntervalMilliseconds: 1000,
		},
		Common: &models.Common{
			Directory: dir,
			Namespace: testNamespace,
			Parallel:  1,
		},
		Compression: &models.Compression{
			Mode: backup.CompressNone,
		},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
		AwsS3:       &models.AwsS3{},
		GcpStorage:  &models.GcpStorage{},
		AzureBlob:   &models.AzureBlob{},
	}

	err := createRecords(asbParams.ClientConfig, asbParams.ClientPolicy, testNamespace, testSet)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	asb, err := appBackup.NewService(ctx, asbParams, logger)
	require.NoError(t, err)

	err = asb.Run(ctx)
	require.NoError(t, err)

	asrParams := &config2.RestoreParams{
		App: &models.App{},
		ClientConfig: &client.AerospikeConfig{
			Seeds: client.HostTLSPortSlice{
				hostPort,
			},
			User:     testASLoginPassword,
			Password: testASLoginPassword,
		},
		ClientPolicy: &models.ClientPolicy{
			Timeout:      1000,
			IdleTimeout:  1000,
			LoginTimeout: 1000,
		},
		Restore: &models.Restore{
			BatchSize:       1,
			MaxAsyncBatches: 1,
			Mode:            models.RestoreModeASB,
		},
		Common: &models.Common{
			Directory: dir,
			Namespace: testNamespace,
			Parallel:  1,
		},
		Compression: &models.Compression{
			Mode: backup.CompressNone,
		},
		Encryption:  &models.Encryption{},
		SecretAgent: &models.SecretAgent{},
		AwsS3: &models.AwsS3{
			RestorePollDuration: 1000,
		},
		GcpStorage: &models.GcpStorage{},
		AzureBlob: &models.AzureBlob{
			RestorePollDuration: 1000,
		},
	}

	asr, err := NewService(ctx, asrParams, logger)
	require.NoError(t, err)

	err = asr.Run(ctx)
	require.NoError(t, err)
}

func createRecords(cfg *client.AerospikeConfig, cp *models.ClientPolicy, namespace, set string) error {
	client, err := storage.NewAerospikeClient(cfg, cp, "", 0, slog.Default())
	if err != nil {
		return fmt.Errorf("failed to create aerospike client: %w", err)
	}

	wp := aerospike.NewWritePolicy(0, 0)

	for i := 0; i < 10; i++ {
		key, err := aerospike.NewKey(namespace, set, fmt.Sprintf("map-key-%d", i))
		if err != nil {
			return fmt.Errorf("failed to create aerospike key: %w", err)
		}

		bin := aerospike.NewBin("time", time.Now().Unix())

		if err = client.PutBins(wp, key, bin); err != nil {
			return fmt.Errorf("failed to create aerospike key: %w", err)
		}
	}

	return nil
}

func TestGetWarmUp(t *testing.T) {
	tests := []struct {
		name            string
		warmUp          int
		maxAsyncBatches int
		expected        int
	}{
		{
			name:            "Default case with zero warmUp",
			warmUp:          0,
			maxAsyncBatches: 10,
			expected:        11,
		},
		{
			name:            "Custom warmUp value",
			warmUp:          5,
			maxAsyncBatches: 10,
			expected:        5,
		},
		{
			name:            "Zero maxAsyncBatches with zero warmUp",
			warmUp:          0,
			maxAsyncBatches: 0,
			expected:        1,
		},
		{
			name:            "Large maxAsyncBatches with zero warmUp",
			warmUp:          0,
			maxAsyncBatches: 1000,
			expected:        1001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetWarmUp(tt.warmUp, tt.maxAsyncBatches)
			if result != tt.expected {
				t.Errorf("getWarmUp(%d, %d) = %d, want %d",
					tt.warmUp, tt.maxAsyncBatches, result, tt.expected)
			}
		})
	}
}
