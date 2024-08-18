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

	writer, err := NewWriter(ctx, client, testBucketName, testFolderName, true)
	require.NoError(t, err)

	w, err := writer.NewWriter(ctx, testFileName)
	require.NoError(t, err)

	n, err := w.Write([]byte(testFileContent))
	require.NoError(t, err)

	require.Equal(t, len(testFileContent), n)

	err = w.Close()
	require.NoError(t, err)
}

func TestWriter_isEmptyDirectory(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	require.NoError(t, err)

	isEmpty, err := isEmptyDirectory(ctx, client, testBucketName, "empty_folder")
	require.NoError(t, err)

	t.Log(isEmpty)
}
