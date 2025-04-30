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

package compression

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockWriteCloser is a mock implementation of io.WriteCloser for testing
type mockWriteCloser struct {
	writeFunc func([]byte) (int, error)
	closeFunc func() error
}

func (m *mockWriteCloser) Write(p []byte) (int, error) {
	return m.writeFunc(p)
}

func (m *mockWriteCloser) Close() error {
	return m.closeFunc()
}

// mockZstdWriter is a mock implementation of the zstd writer for testing
type mockZstdWriter struct {
	io.WriteCloser
	closeErr error
}

func (m *mockZstdWriter) Close() error {
	return m.closeErr
}

func TestNewWriter(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		mockW := &mockWriteCloser{
			writeFunc: func(p []byte) (int, error) { return len(p), nil },
			closeFunc: func() error { return nil },
		}

		w, err := NewWriter(mockW, 1)
		require.NoError(t, err)
		require.NotNil(t, w)

		_, ok := w.(*writer)
		assert.True(t, ok)
	})

	t.Run("Invalid compression level", func(t *testing.T) {
		mockW := &mockWriteCloser{
			writeFunc: func(p []byte) (int, error) { return len(p), nil },
			closeFunc: func() error { return nil },
		}

		w, err := NewWriter(mockW, 100)

		require.NoError(t, err)
		require.NotNil(t, w)
	})
}

func TestWrite(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		var buf bytes.Buffer
		bufCloser := &mockWriteCloser{
			writeFunc: buf.Write,
			closeFunc: func() error { return nil },
		}

		w, err := NewWriter(bufCloser, 1)
		require.NoError(t, err)

		testData := []byte("test data to compress")
		n, err := w.Write(testData)
		require.NoError(t, err)
		require.Equal(t, len(testData), n)

		err = w.Close()
		require.NoError(t, err)

		decoder, err := zstd.NewReader(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)
		defer decoder.Close()

		decompressed, err := io.ReadAll(decoder)
		require.NoError(t, err)
		require.Equal(t, testData, decompressed)
	})
}

func TestClose(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		mockW := &mockWriteCloser{
			writeFunc: func(p []byte) (int, error) { return len(p), nil },
			closeFunc: func() error { return nil },
		}

		w, err := NewWriter(mockW, 1)
		require.NoError(t, err)

		err = w.Close()
		require.NoError(t, err)
	})

	t.Run("Error closing zstd writer", func(t *testing.T) {
		mockW := &mockWriteCloser{
			writeFunc: func(p []byte) (int, error) { return len(p), nil },
			closeFunc: func() error { return nil },
		}

		w, err := NewWriter(mockW, 1)
		require.NoError(t, err)

		cw := w.(*writer)
		expectedErr := errors.New("zstd close error")
		cw.zstdWriter = &mockZstdWriter{
			WriteCloser: cw.zstdWriter,
			closeErr:    expectedErr,
		}

		err = w.Close()
		require.Error(t, err)
		require.Contains(t, err.Error(), expectedErr.Error())
	})

	t.Run("Error closing underlying writer", func(t *testing.T) {
		expectedErr := errors.New("close error")
		mockW := &mockWriteCloser{
			writeFunc: func(p []byte) (int, error) { return len(p), nil },
			closeFunc: func() error { return expectedErr },
		}

		w, err := NewWriter(mockW, 1)
		require.NoError(t, err)

		err = w.Close()
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
	})
}

func TestRealCompression(t *testing.T) {
	var compressedBuf bytes.Buffer
	bufCloser := &mockWriteCloser{
		writeFunc: compressedBuf.Write,
		closeFunc: func() error { return nil },
	}

	w, err := NewWriter(bufCloser, 3)
	require.NoError(t, err)

	testData := bytes.Repeat([]byte("This is a test string with some repetition. "), 100)

	n, err := w.Write(testData)
	require.NoError(t, err)
	require.Equal(t, len(testData), n)

	err = w.Close()
	require.NoError(t, err)

	require.Less(t, compressedBuf.Len(), len(testData), "Compressed data should be smaller than original")

	decoder, err := zstd.NewReader(bytes.NewReader(compressedBuf.Bytes()))
	require.NoError(t, err)
	defer decoder.Close()

	decompressed, err := io.ReadAll(decoder)
	require.NoError(t, err)
	require.Equal(t, testData, decompressed, "Decompressed data should match original")
}
