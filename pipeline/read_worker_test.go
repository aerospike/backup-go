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

package pipeline

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aerospike/backup-go/pipeline/mocks"
	"github.com/stretchr/testify/suite"
)

type readersTestSuite struct {
	suite.Suite
}

func TestReaders(t *testing.T) {
	suite.Run(t, new(readersTestSuite))
}

func (suite *readersTestSuite) TestReadWorker() {
	suite.T().Parallel()
	mockReader := mocks.NewMockdataReader[string](suite.T())

	readCalls := 0
	mockReader.EXPECT().Read().RunAndReturn(func() (string, error) {
		readCalls++
		if readCalls <= 3 {
			return "hi", nil
		}
		return "", io.EOF
	})
	mockReader.EXPECT().Close()

	worker := NewReadWorker[string](mockReader)
	suite.NotNil(worker)

	send := make(chan string, 3)
	worker.SetSendChan(send)

	ctx := context.Background()
	err := worker.Run(ctx)
	suite.Nil(err)
	close(send)

	suite.Equal(3, len(send))

	for v := range send {
		suite.Equal("hi", v)
	}
}

func (suite *readersTestSuite) TestReadWorkerClose() {
	suite.T().Parallel()
	mockReader := mocks.NewMockdataReader[string](suite.T())
	mockReader.EXPECT().Read().Return("hi", nil)
	mockReader.EXPECT().Close()

	worker := NewReadWorker[string](mockReader)
	suite.NotNil(worker)

	ctx, cancel := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := worker.Run(ctx)
		suite.NotNil(err)
	}()

	// give the worker some time to start
	time.Sleep(100 * time.Millisecond)

	cancel()
	wg.Wait()
}
