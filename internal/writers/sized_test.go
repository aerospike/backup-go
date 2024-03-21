// Copyright 2024-2024 Aerospike, Inc.
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

package writers_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/aerospike/backup-go/internal/writers"
	"github.com/stretchr/testify/suite"
)

type SizedTestSuite struct {
	suite.Suite
	namespace string
}

type mockWriteCloser struct {
	io.Writer
	closed bool
}

func (m *mockWriteCloser) Close() error {
	m.closed = true
	return nil
}

func (suite *SizedTestSuite) Test_writeCloserSized() {
	writer := mockWriteCloser{
		Writer: &bytes.Buffer{},
	}

	open := func() (io.WriteCloser, error) {
		w := mockWriteCloser{
			Writer: &bytes.Buffer{},
		}

		return &w, nil
	}

	wcs := writers.NewSized(10, &writer, open)
	suite.NotNil(wcs)

	n, err := wcs.Write([]byte("test"))
	suite.NoError(err)
	suite.Equal(4, n)

	suite.False(writer.closed)

	// cross the limit here
	n, err = wcs.Write([]byte("0123456789"))
	suite.NoError(err)
	suite.Equal(10, n)

	n, err = wcs.Write([]byte("test1"))
	suite.NoError(err)
	suite.Equal(5, n)

	suite.True(writer.closed)

	suite.Equal("test0123456789", writer.Writer.(*bytes.Buffer).String())
	suite.Equal("test1", wcs.WriteCloser.(*mockWriteCloser).Writer.(*bytes.Buffer).String())
}

func Test_SizedTestSuite(t *testing.T) {
	suite.Run(t, new(SizedTestSuite))
}
