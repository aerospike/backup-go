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
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/tools-common-go/client"
	"github.com/stretchr/testify/require"
)

const (
	testNamespace = "test"
	testSet       = "test"
	testStateFile = "state"
)

// Test_BackupRestore one test for both so we can restore from just backed-up files.
func Test_BackupRestore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	hostPort := client.NewDefaultHostTLSPort()

	asbParams := &ASBackupParams{
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
		BackupParams: &models.Backup{
			InfoMaxRetries:                3,
			InfoRetriesMultiplier:         1,
			InfoRetryIntervalMilliseconds: 1000,
		},
		CommonParams: &models.Common{
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

	asb, err := NewASBackup(ctx, asbParams, logger)
	require.NoError(t, err)

	err = asb.Run(ctx)
	require.NoError(t, err)

	asrParams := &ASRestoreParams{
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
		RestoreParams: &models.Restore{
			BatchSize:       1,
			MaxAsyncBatches: 1,
			Mode:            models.RestoreModeASB,
		},
		CommonParams: &models.Common{
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

	asr, err := NewASRestore(ctx, asrParams, logger)
	require.NoError(t, err)

	err = asr.Run(ctx)
	require.NoError(t, err)
}

func Test_BackupWithState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	hostPort := client.NewDefaultHostTLSPort()

	asbParams := &ASBackupParams{
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
		BackupParams: &models.Backup{
			StateFileDst:                  testStateFile,
			ScanPageSize:                  10,
			FileLimit:                     100000,
			InfoMaxRetries:                3,
			InfoRetriesMultiplier:         1,
			InfoRetryIntervalMilliseconds: 1000,
		},
		CommonParams: &models.Common{
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

	asb, err := NewASBackup(ctx, asbParams, logger)
	require.NoError(t, err)

	err = asb.Run(ctx)
	require.NoError(t, err)
}

func createRecords(cfg *client.AerospikeConfig, cp *models.ClientPolicy, namespace, set string) error {
	client, err := newAerospikeClient(cfg, cp, "")
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
