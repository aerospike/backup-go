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

	// Check restore.
	newCtx := context.Background()
	newState, err := NewStateFromFile(newCtx, tempFile, logger)
	require.NoError(t, err)
	require.Equal(t, newState.PartitionFilters, pfs)
}
