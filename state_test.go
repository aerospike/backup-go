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

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/stretchr/testify/require"
)

const (
	testDuration = 1 * time.Second
)

func TestState(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	tempFile := filepath.Join(t.TempDir(), "state_test.gob")
	pfs := []*a.PartitionFilter{
		NewPartitionFilterByID(1),
		NewPartitionFilterByID(2),
	}
	logger := slog.New(slog.NewTextHandler(nil, nil))

	// Check init.
	state := NewState(ctx, tempFile, testDuration, pfs, logger)

	time.Sleep(testDuration * 3)

	require.NotZero(t, state.SavedAt)
	cancel()

	// Check that file exists.
	_, err := os.Stat(tempFile)
	require.NoError(t, err)

	// Nullify the link.
	pfs = nil
	result := []*a.PartitionFilter{
		NewPartitionFilterByID(1),
		NewPartitionFilterByID(2),
	}

	// Check restore.
	newCtx := context.Background()
	newState, err := NewStateFromFile(newCtx, tempFile, logger)
	require.NoError(t, err)
	require.Equal(t, newState.PartitionFilters, result)
}
