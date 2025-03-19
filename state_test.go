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
	"os"
	"path/filepath"
	"testing"
	"time"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/io/encoding/asb"
	ioStorage "github.com/aerospike/backup-go/io/storage"
	"github.com/aerospike/backup-go/io/storage/local"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"github.com/stretchr/testify/require"
)

const (
	testStateFile = "test_state_file"
)

func TestState(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	tempFile := filepath.Join(testDir, testStateFile)

	testFilters := []*a.PartitionFilter{
		NewPartitionFilterByID(1),
		NewPartitionFilterByID(2),
	}

	ctx := context.Background()

	cfg := NewDefaultBackupConfig()
	cfg.StateFile = testStateFile
	cfg.PageSize = 100000
	cfg.PipelinesMode = pipeline.ModeParallel
	cfg.PartitionFilters = testFilters

	reader, err := local.NewReader(
		ctx,
		ioStorage.WithDir(testDir),
		ioStorage.WithSkipDirCheck(),
	)
	require.NoError(t, err)

	writer, err := local.NewWriter(
		ctx,
		ioStorage.WithValidator(asb.NewValidator()),
		ioStorage.WithSkipDirCheck(),
		ioStorage.WithDir(testDir),
	)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Check init.
	state, err := NewState(ctx, cfg, reader, writer, logger)
	require.NotNil(t, state)
	require.NoError(t, err)

	err = state.initState(testFilters)
	require.NoError(t, err)

	state.Counter = 1
	suf := state.getFileSuffix()
	require.Equal(t, "(1)", suf)

	// Check empty condition.
	state.RecordsStateChan <- models.PartitionFilterSerialized{}
	for i := range testFilters {
		pfs, err := models.NewPartitionFilterSerialized(testFilters[i])
		require.NoError(t, err)
		state.RecordsStateChan <- pfs
	}

	// Create a state file.
	state.SaveCommandChan <- 1
	time.Sleep(1 * time.Second)

	// Check that file exists.
	_, err = os.Stat(tempFile)
	require.NoError(t, err)

	// Check restore.
	newCtx := context.Background()
	cfg.Continue = true
	newState, err := NewState(newCtx, cfg, reader, writer, logger)
	require.NoError(t, err)
	newPf, err := newState.loadPartitionFilters()
	require.NoError(t, err)
	require.Equal(t, len(testFilters), len(newPf))
}
