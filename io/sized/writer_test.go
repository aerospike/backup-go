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

	"github.com/stretchr/testify/suite"
)

type sizedTestSuite struct {
	suite.Suite
}

type mockWriteCloser struct {
	io.Writer
	closed     bool
	closeError error
}

func (m *mockWriteCloser) Close() error {
	m.closed = true
	return m.closeError
}

func Test_SizedTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(sizedTestSuite))
}

func (suite *sizedTestSuite) Test_writeCloserSized() {
	suite.T().Parallel()
	var writer1 *mockWriteCloser
	var writer2 *mockWriteCloser

	open := func(_ context.Context, _ string, _ *atomic.Uint64) (io.WriteCloser, error) {
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
	suite.NotNil(wcs)
	suite.Nil(err)

	defer wcs.Close()

	n, err := wcs.Write([]byte("test"))
	wcs.sizeCounter.Add(4)
	suite.NoError(err)
	suite.Equal(4, n)

	suite.NotNil(writer1)
	suite.False(writer1.closed)
	suite.Equal(writer1, wcs.writer)

	// cross the limit here
	n, err = wcs.Write([]byte("0123456789"))
	wcs.sizeCounter.Add(10)
	suite.NoError(err)
	suite.Equal(10, n)

	n, err = wcs.Write([]byte("test1"))
	wcs.sizeCounter.Add(5)
	suite.NoError(err)
	suite.Equal(5, n)

	suite.True(writer1.closed)
	suite.NotNil(writer2)
	suite.Equal(writer2, wcs.writer)

	suite.Equal("test0123456789", writer1.Writer.(*bytes.Buffer).String())
	suite.Equal("test1", writer2.Writer.(*bytes.Buffer).String())
}

func (suite *sizedTestSuite) Test_writeCloserSized_WithSaveCommandChan() {
	suite.T().Parallel()
	var writer1 *mockWriteCloser
	var writer2 *mockWriteCloser

	saveCommandChan := make(chan int, 1)

	open := func(_ context.Context, _ string, _ *atomic.Uint64) (io.WriteCloser, error) {
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
	suite.NotNil(wcs)
	suite.Nil(err)

	defer wcs.Close()

	n, err := wcs.Write([]byte("test"))
	suite.NoError(err)
	suite.Equal(4, n)

	wcs.sizeCounter.Store(11)

	n, err = wcs.Write([]byte("next"))
	suite.NoError(err)
	suite.Equal(4, n)

	select {
	case cmd := <-saveCommandChan:
		suite.Equal(5, cmd)
	default:
		suite.Fail("Expected save command to be sent")
	}
}

func (suite *sizedTestSuite) Test_writeCloserSized_CloseError() {
	suite.T().Parallel()
	var writer1 *mockWriteCloser

	open := func(_ context.Context, _ string, _ *atomic.Uint64) (io.WriteCloser, error) {
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
	suite.NotNil(wcs)
	suite.Nil(err)

	n, err := wcs.Write([]byte("test"))
	wcs.sizeCounter.Add(4)
	suite.NoError(err)
	suite.Equal(4, n)

	// cross the limit here to trigger close error
	wcs.sizeCounter.Add(10)
	_, err = wcs.Write([]byte("test"))
	suite.Error(err)
	suite.Contains(err.Error(), "close error")
}

func (suite *sizedTestSuite) Test_writeCloserSized_OpenError() {
	suite.T().Parallel()

	open := func(_ context.Context, _ string, _ *atomic.Uint64) (io.WriteCloser, error) {
		return nil, fmt.Errorf("open error")
	}

	wcs, err := NewWriter(context.Background(), 1, nil, 10, open)
	suite.NotNil(wcs)
	suite.Nil(err)

	_, err = wcs.Write([]byte("test"))
	suite.Error(err)
	suite.Contains(err.Error(), "open error")
}

func (suite *sizedTestSuite) Test_writeCloserSized_CloseNilWriter() {
	suite.T().Parallel()

	open := func(_ context.Context, _ string, _ *atomic.Uint64) (io.WriteCloser, error) {
		return nil, fmt.Errorf("open error")
	}

	wcs, err := NewWriter(context.Background(), 1, nil, 10, open)
	suite.NotNil(wcs)
	suite.Nil(err)

	err = wcs.Close()
	suite.NoError(err)
}
