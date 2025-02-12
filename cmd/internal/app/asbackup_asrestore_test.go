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

// Test_BackupRestore one test for both so we can restore from just backed up files.
func Test_BackupRestore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()

	hostPort := client.NewDefaultHostTLSPort()
	clientCfg := &client.AerospikeConfig{
		Seeds: client.HostTLSPortSlice{
			hostPort,
		},
		User:     testASLoginPassword,
		Password: testASLoginPassword,
	}

	clientPolicy := &models.ClientPolicy{
		Timeout:      1000,
		IdleTimeout:  1000,
		LoginTimeout: 1000,
	}

	bParams := &models.Backup{}

	cParams := &models.Common{
		Directory: dir,
		Namespace: testNamespace,
		Parallel:  1,
	}

	comp := &models.Compression{
		Mode: backup.CompressNone,
	}

	enc := &models.Encryption{}

	sa := &models.SecretAgent{}

	aws := &models.AwsS3{}

	gcp := &models.GcpStorage{}

	azure := &models.AzureBlob{}

	err := createRecords(clientCfg, clientPolicy, testNamespace, testSet)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	asb, err := NewASBackup(ctx, clientCfg, clientPolicy, bParams, cParams, comp, enc, sa, aws, gcp, azure, logger)
	require.NoError(t, err)

	err = asb.Run(ctx)
	require.NoError(t, err)

	rParams := &models.Restore{
		BatchSize:       1,
		MaxAsyncBatches: 1,
	}

	asr, err := NewASRestore(ctx, clientCfg, clientPolicy, rParams, cParams, comp, enc, sa, aws, gcp, azure, logger)
	require.NoError(t, err)

	err = asr.Run(ctx)
	require.NoError(t, err)
}

func Test_BackupWithState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()

	hostPort := client.NewDefaultHostTLSPort()
	clientCfg := &client.AerospikeConfig{
		Seeds: client.HostTLSPortSlice{
			hostPort,
		},
		User:     testASLoginPassword,
		Password: testASLoginPassword,
	}

	clientPolicy := &models.ClientPolicy{
		Timeout:      1000,
		IdleTimeout:  1000,
		LoginTimeout: 1000,
	}

	bParams := &models.Backup{
		StateFileDst: testStateFile,
		ScanPageSize: 10,
		FileLimit:    100000,
	}

	cParams := &models.Common{
		Directory: dir,
		Namespace: testNamespace,
		Parallel:  1,
	}

	comp := &models.Compression{
		Mode: backup.CompressNone,
	}

	enc := &models.Encryption{}

	sa := &models.SecretAgent{}

	aws := &models.AwsS3{}

	gcp := &models.GcpStorage{}

	azure := &models.AzureBlob{}

	err := createRecords(clientCfg, clientPolicy, testNamespace, testSet)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	asb, err := NewASBackup(ctx, clientCfg, clientPolicy, bParams, cParams, comp, enc, sa, aws, gcp, azure, logger)
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
