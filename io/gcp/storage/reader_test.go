package storage

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
)

const (
	testBucketName = "backup-go-tests"
	testFolderName = "test/"
)

type validatorMock struct{}

func (mock validatorMock) Run(fileName string) error {
	if !strings.HasSuffix(fileName, ".asb") {
		return fmt.Errorf("file name must end with .asb")
	}
	return nil
}

func TestStreamingReader_StreamFilesFolder(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	require.NoError(t, err)

	reader, err := NewStreamingReader(client, testBucketName, testFolderName, validatorMock{})
	require.NoError(t, err)

	rCH := make(chan io.ReadCloser)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH)

	for msg := range rCH {
		t.Log(msg)
	}
}

func TestStreamingReader_StreamFilesRoot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	require.NoError(t, err)

	reader, err := NewStreamingReader(client, testBucketName, "", validatorMock{})
	require.NoError(t, err)

	rCH := make(chan io.ReadCloser)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH)

	for msg := range rCH {
		t.Log(msg)
	}
}
