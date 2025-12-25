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
	"runtime"
	"testing"
	"time"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aerospike/backup-go/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestBackupHandler_GoroutineLeak_OnSuccess(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	stateFile := filepath.Join(testDir, "state_file")

	// Count goroutines before test
	initialGoroutines := runtime.NumGoroutine()

	// Setup mocks
	mockAerospikeClient := mocks.NewMockAerospikeClient(t)
	mockWriter := mocks.NewMockWriter(t)
	mockReader := mocks.NewMockStreamingReader(t)
	mockInfoGetter := mocks.NewMockInfoGetter(t)

	// Configure mock expectations
	mockWriter.EXPECT().GetType().Return("local").Maybe()
	mockWriter.EXPECT().GetOptions().Return(options.Options{
		IsDir: true,
	}).Maybe()
	mockWriter.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockInfoGetter.EXPECT().HasExpressionSIndex(mock.Anything, mock.Anything).Return(false, nil).Maybe()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := NewDefaultBackupConfig()
	cfg.Namespace = "test"
	cfg.StateFile = stateFile
	cfg.PageSize = 100000
	cfg.PartitionFilters = []*a.PartitionFilter{
		NewPartitionFilterByID(1),
	}
	cfg.NoRecords = true // Skip actual backup
	cfg.NoUDFs = true
	cfg.NoIndexes = true

	// Create backup handler
	handler, err := newBackupHandler(
		context.Background(),
		cfg,
		mockAerospikeClient,
		logger,
		mockWriter,
		mockReader,
		nil, // scanLimiter
		mockInfoGetter,
	)
	require.NoError(t, err)
	require.NotNil(t, handler)

	// Simulate successful backup completion
	// This is what happens in handler.run() when backup succeeds
	handler.done <- struct{}{}

	// Call Wait which should clean up
	err = handler.Wait(context.Background())
	require.NoError(t, err)

	// Give goroutines time to exit
	time.Sleep(200 * time.Millisecond)
	runtime.GC()

	// Count goroutines after cleanup
	finalGoroutines := runtime.NumGoroutine()
	leaked := finalGoroutines - initialGoroutines

	// Spawned goroutines should be cleaned up
	require.LessOrEqual(t, leaked, 0,
		"goroutine leak detected: started with %d, ended with %d (leaked %d).",
		initialGoroutines, finalGoroutines, leaked)
}
