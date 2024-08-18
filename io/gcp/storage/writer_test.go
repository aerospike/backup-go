package storage

import (
	"context"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
)

const (
	testFileName    = "test/test.asb"
	testFileContent = "content"
)

func TestWriter_NewWriter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	require.NoError(t, err)
	cfg := testConfig()

	writer, err := NewWriter(ctx, client, cfg, true)
	require.NoError(t, err)

	w, err := writer.NewWriter(ctx, testFileName)
	require.NoError(t, err)

	n, err := w.Write([]byte(testFileContent))
	require.NoError(t, err)

	require.Equal(t, len(testFileContent), n)

	err = w.Close()
	require.NoError(t, err)
}
