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

package std

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/require"
)

const (
	testPath     = "/test/path"
	testData     = "test data\n"
	testFileName = "stdin.asb"
)

func TestReader_GetType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, err := NewReader(ctx)
	require.NoError(t, err)

	require.Equal(t, stdinType, r.GetType())
}

func TestReader_GetSize(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, err := NewReader(ctx)
	require.NoError(t, err)

	require.Equal(t, int64(-1), r.GetSize())
}

func TestReader_GetNumber(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, err := NewReader(ctx)
	require.NoError(t, err)

	require.Equal(t, int64(-1), r.GetNumber())
}

func TestReader_CtxCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err := NewReader(ctx)
	require.Error(t, err)
	require.Nil(t, r)
}

func TestReader_ListObjects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, err := NewReader(ctx)
	require.NoError(t, err)

	objects, err := r.ListObjects(ctx, testPath)
	require.NoError(t, err)
	require.Equal(t, []string{stdinType}, objects)

	ctx, cancel := context.WithCancel(ctx)
	cancel()
	objects, err = r.ListObjects(ctx, testPath)
	require.Error(t, err)
	require.Empty(t, objects)
}

func TestReader_StreamFiles(t *testing.T) {
	t.Parallel()

	oldStdin := os.Stdin
	r, w, _ := os.Pipe()
	os.Stdin = r
	defer func() { os.Stdin = oldStdin }()

	go func() {
		defer w.Close()
		_, err := w.WriteString(testData)
		require.NoError(t, err)
	}()

	ctx := context.Background()
	reader, err := NewReader(ctx)
	require.NoError(t, err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go reader.StreamFiles(ctx, readerChan, errorChan)

	var counter int
	for {
		select {
		case file, ok := <-readerChan:
			if !ok {
				require.Equal(t, 1, counter)
				return
			}
			counter++

			require.Equal(t, stdinType, file.Name)
			require.NotNil(t, file.Reader)

			data, err := io.ReadAll(file.Reader)
			require.NoError(t, err)
			require.Equal(t, testData, string(data))

		case err := <-errorChan:
			require.NoError(t, err)
		}
	}
}

func TestReader_StreamFile(t *testing.T) {
	t.Parallel()

	oldStdin := os.Stdin
	r, w, _ := os.Pipe()
	os.Stdin = r
	defer func() { os.Stdin = oldStdin }()

	go func() {
		defer w.Close()
		_, err := w.WriteString(testData)
		require.NoError(t, err)
	}()

	ctx := context.Background()
	reader, err := NewReader(ctx)
	require.NoError(t, err)

	readerChan := make(chan models.File, 1)
	errorChan := make(chan error, 1)

	go reader.StreamFile(ctx, testFileName, readerChan, errorChan)

	select {
	case file := <-readerChan:
		require.Equal(t, testFileName, file.Name)
		require.NotNil(t, file.Reader)

		data, err := io.ReadAll(file.Reader)
		require.NoError(t, err)
		require.Equal(t, testData, string(data))

	case err := <-errorChan:
		require.NoError(t, err)
	}
}

func TestStdinReadCloser(t *testing.T) {
	t.Parallel()

	oldStdin := os.Stdin
	r, w, _ := os.Pipe()
	os.Stdin = r
	defer func() { os.Stdin = oldStdin }()

	go func() {
		defer w.Close()
		_, err := w.WriteString(testData)
		require.NoError(t, err)
	}()

	readCloser := newStdinReadCloser()
	require.NotNil(t, readCloser)

	data, err := io.ReadAll(readCloser)
	require.NoError(t, err)
	require.Equal(t, testData, string(data))

	err = readCloser.Close()
	require.NoError(t, err)
}

func TestReader_EmptyStdin(t *testing.T) {
	t.Parallel()

	oldStdin := os.Stdin
	r, w, _ := os.Pipe()
	os.Stdin = r
	defer func() { os.Stdin = oldStdin }()

	w.Close()

	ctx := context.Background()
	reader, err := NewReader(ctx)
	require.NoError(t, err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go reader.StreamFiles(ctx, readerChan, errorChan)

	var counter int
	for {
		select {
		case file, ok := <-readerChan:
			if !ok {
				require.Equal(t, 1, counter)
				return
			}
			counter++

			require.Equal(t, stdinType, file.Name)
			require.NotNil(t, file.Reader)

			data, err := io.ReadAll(file.Reader)
			require.NoError(t, err)
			require.Empty(t, string(data))

		case err := <-errorChan:
			require.NoError(t, err)
		}
	}
}
