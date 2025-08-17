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

package sized

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockWriteCloser struct {
	io.Writer
	closed     bool
	closeError error
}

func (m *mockWriteCloser) Close() error {
	m.closed = true
	return m.closeError
}

func Test_writeCloserSized(t *testing.T) {
	t.Parallel()
	var writer1 *mockWriteCloser
	var writer2 *mockWriteCloser

	open := func(_ context.Context, _ int, _ *atomic.Uint64) (io.WriteCloser, error) {
		if writer1 == nil {
			writer1 = &mockWriteCloser{
				Writer: &bytes.Buffer{},
			}

			return writer1, nil
		}
		writer2 = &mockWriteCloser{
			Writer: &bytes.Buffer{},
		}

		return writer2, nil
	}

	wcs, err := NewWriter(context.Background(), 1, nil, 10, open)
	require.NotNil(t, wcs)
	require.Nil(t, err)

	defer wcs.Close()

	n, err := wcs.Write([]byte("test"))
	wcs.sizeCounter.Add(4)
	require.NoError(t, err)
	require.Equal(t, 4, n)

	require.NotNil(t, writer1)
	require.False(t, writer1.closed)
	require.Equal(t, writer1, wcs.writer)

	// cross the limit here
	n, err = wcs.Write([]byte("0123456789"))
	wcs.sizeCounter.Add(10)
	require.NoError(t, err)
	require.Equal(t, 10, n)

	n, err = wcs.Write([]byte("test1"))
	wcs.sizeCounter.Add(5)
	require.NoError(t, err)
	require.Equal(t, 5, n)

	require.True(t, writer1.closed)
	require.NotNil(t, writer2)
	require.Equal(t, writer2, wcs.writer)

	require.Equal(t, "test0123456789", writer1.Writer.(*bytes.Buffer).String())
	require.Equal(t, "test1", writer2.Writer.(*bytes.Buffer).String())
}

func Test_writeCloserSized_WithSaveCommandChan(t *testing.T) {
	t.Parallel()
	var writer1 *mockWriteCloser
	var writer2 *mockWriteCloser

	saveCommandChan := make(chan int, 1)

	open := func(_ context.Context, _ int, _ *atomic.Uint64) (io.WriteCloser, error) {
		if writer1 == nil {
			writer1 = &mockWriteCloser{
				Writer: &bytes.Buffer{},
			}

			return writer1, nil
		}
		writer2 = &mockWriteCloser{
			Writer: &bytes.Buffer{},
		}

		return writer2, nil
	}

	wcs, err := NewWriter(context.Background(), 5, saveCommandChan, 10, open)
	require.NotNil(t, wcs)
	require.Nil(t, err)

	defer wcs.Close()

	n, err := wcs.Write([]byte("test"))
	require.NoError(t, err)
	require.Equal(t, 4, n)

	wcs.sizeCounter.Store(11)

	n, err = wcs.Write([]byte("next"))
	require.NoError(t, err)
	require.Equal(t, 4, n)

	select {
	case cmd := <-saveCommandChan:
		require.Equal(t, 5, cmd)
	default:
		require.Fail(t, "Expected save command to be sent")
	}
}

func Test_writeCloserSized_CloseError(t *testing.T) {
	t.Parallel()
	var writer1 *mockWriteCloser

	open := func(_ context.Context, _ int, _ *atomic.Uint64) (io.WriteCloser, error) {
		if writer1 == nil {
			writer1 = &mockWriteCloser{
				Writer:     &bytes.Buffer{},
				closeError: fmt.Errorf("close error"),
			}

			return writer1, nil
		}
		return nil, fmt.Errorf("should not be called")
	}

	wcs, err := NewWriter(context.Background(), 1, nil, 10, open)
	require.NotNil(t, wcs)
	require.Nil(t, err)

	n, err := wcs.Write([]byte("test"))
	wcs.sizeCounter.Add(4)
	require.NoError(t, err)
	require.Equal(t, 4, n)

	// cross the limit here to trigger close error
	wcs.sizeCounter.Add(10)
	_, err = wcs.Write([]byte("test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "close error")
}

func Test_writeCloserSized_OpenError(t *testing.T) {
	t.Parallel()

	open := func(_ context.Context, _ int, _ *atomic.Uint64) (io.WriteCloser, error) {
		return nil, fmt.Errorf("open error")
	}

	wcs, err := NewWriter(context.Background(), 1, nil, 10, open)
	require.NotNil(t, wcs)
	require.Nil(t, err)

	_, err = wcs.Write([]byte("test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "open error")
}

func Test_writeCloserSized_CloseNilWriter(t *testing.T) {
	t.Parallel()

	open := func(_ context.Context, _ int, _ *atomic.Uint64) (io.WriteCloser, error) {
		return nil, fmt.Errorf("open error")
	}

	wcs, err := NewWriter(context.Background(), 1, nil, 10, open)
	require.NotNil(t, wcs)
	require.Nil(t, err)

	err = wcs.Close()
	require.NoError(t, err)
}
