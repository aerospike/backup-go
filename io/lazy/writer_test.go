package lazy

import (
	"context"
	"io"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

const testFileName = "test"

func TestLazyWriter(t *testing.T) {
	ctx := context.Background()

	filePath := path.Join(t.TempDir(), testFileName)

	openFunc := func(_ context.Context) (io.WriteCloser, error) {
		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0o666)
		if err != nil {
			return nil, err
		}
		return file, nil
	}

	writer, err := NewWriter(ctx, openFunc)
	require.NoError(t, err)

	// Check that file not exist.
	_, err = os.Stat(filePath)
	require.ErrorIs(t, err, os.ErrNotExist)

	_, err = writer.Write([]byte("Hello, World!"))
	require.NoError(t, err)

	// Now a file exists.
	_, err = os.Stat(filePath)
	require.NoError(t, err)
}
