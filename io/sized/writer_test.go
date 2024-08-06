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
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type sizedTestSuite struct {
	suite.Suite
}

type mockWriteCloser struct {
	io.Writer
	closed bool
}

func (m *mockWriteCloser) Close() error {
	m.closed = true
	return nil
}

func Test_SizedTestSuite(t *testing.T) {
	suite.Run(t, new(sizedTestSuite))
}

func (suite *sizedTestSuite) Test_writeCloserSized() {
	var writer1 *mockWriteCloser
	var writer2 *mockWriteCloser

	open := func(_ context.Context) (io.WriteCloser, error) {
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

	wcs, err := NewWriter(context.Background(), 10, open)
	suite.NotNil(wcs)
	suite.Nil(err)

	defer wcs.Close()

	n, err := wcs.Write([]byte("test"))
	suite.NoError(err)
	suite.Equal(4, n)

	suite.NotNil(writer1)
	suite.False(writer1.closed)
	suite.Equal(writer1, wcs.writer)

	// cross the limit here
	n, err = wcs.Write([]byte("0123456789"))
	suite.NoError(err)
	suite.Equal(10, n)

	n, err = wcs.Write([]byte("test1"))
	suite.NoError(err)
	suite.Equal(5, n)

	suite.True(writer1.closed)
	suite.NotNil(writer2)
	suite.Equal(writer2, wcs.writer)

	suite.Equal("test0123456789", writer1.Writer.(*bytes.Buffer).String())
	suite.Equal("test1", writer2.Writer.(*bytes.Buffer).String())
}

func (suite *sizedTestSuite) Test_writeCloserSized_ErrLimit() {
	var writer1 *mockWriteCloser
	var writer2 *mockWriteCloser

	open := func(_ context.Context) (io.WriteCloser, error) {
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

	_, err := NewWriter(context.Background(), -1, open)
	require.ErrorContains(suite.T(), err, "limit must be greater than 0")
}
