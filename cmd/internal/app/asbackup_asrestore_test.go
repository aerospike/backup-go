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
	"log/slog"
	"os"
	"testing"

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/aerospike/tools-common-go/client"
	"github.com/stretchr/testify/require"
)

const testNamespace = "test"

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

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	asb, err := NewASBackup(ctx, clientCfg, bParams, cParams, comp, enc, sa, aws, gcp, azure, logger)
	require.NoError(t, err)

	err = asb.Run(ctx)
	require.NoError(t, err)

	rParams := &models.Restore{
		BatchSize:       1,
		MaxAsyncBatches: 1,
	}

	asr, err := NewASRestore(ctx, clientCfg, rParams, cParams, comp, enc, sa, aws, gcp, azure, logger)
	require.NoError(t, err)

	err = asr.Run(ctx)
	require.NoError(t, err)
}
