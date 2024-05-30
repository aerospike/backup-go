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

package processors

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aerospike/backup-go/internal/processors/mocks"
	"github.com/stretchr/testify/suite"
)

type processorTestSuite struct {
	suite.Suite
}

func TestProcessors(t *testing.T) {
	suite.Run(t, new(processorTestSuite))
}

func (suite *processorTestSuite) TestProcessorWorker() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("test", nil)

	worker := NewProcessorWorker[string](mockProcessor)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"
	close(receiver)

	worker.SetReceiveChan(receiver)

	sender := make(chan string, 1)
	worker.SetSendChan(sender)

	ctx := context.Background()
	err := worker.Run(ctx)
	suite.Nil(err)

	data := <-sender
	suite.Equal("test", data)
}

func (suite *processorTestSuite) TestProcessorWorkerFilteredOut() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("test", errFilteredOut)

	worker := NewProcessorWorker[string](mockProcessor)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"
	close(receiver)

	worker.SetReceiveChan(receiver)

	sender := make(chan string, 1)
	worker.SetSendChan(sender)

	ctx := context.Background()
	err := worker.Run(ctx)
	suite.Nil(err)

	suite.Equal(0, len(sender))
}

func (suite *processorTestSuite) TestProcessorWorkerCancelOnReceive() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("test", nil)

	worker := NewProcessorWorker[string](mockProcessor)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"

	worker.SetReceiveChan(receiver)

	sender := make(chan string, 1)
	worker.SetSendChan(sender)

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	errorCh := make(chan error, 1)
	go func() {
		defer wg.Done()
		err := worker.Run(ctx)
		errorCh <- err
	}()

	// give the worker some time to start
	time.Sleep(100 * time.Millisecond)

	cancel()
	wg.Wait()

	err := <-errorCh
	suite.NotNil(err)

	data := <-sender
	suite.Equal("test", data)
}

func (suite *processorTestSuite) TestProcessorWorkerCancelOnSend() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("test", nil)

	worker := NewProcessorWorker[string](mockProcessor)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"

	worker.SetReceiveChan(receiver)

	sender := make(chan string)
	worker.SetSendChan(sender)

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	errorCh := make(chan error, 1)
	go func() {
		defer wg.Done()
		err := worker.Run(ctx)
		errorCh <- err
	}()

	// give the worker some time to start
	time.Sleep(100 * time.Millisecond)

	cancel()
	wg.Wait()

	err := <-errorCh
	suite.NotNil(err)
}

func (suite *processorTestSuite) TestProcessorWorkerReceiveClosed() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("test", nil)

	worker := NewProcessorWorker[string](mockProcessor)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"
	close(receiver)

	worker.SetReceiveChan(receiver)

	sender := make(chan string, 1)
	worker.SetSendChan(sender)

	ctx := context.Background()
	err := worker.Run(ctx)
	suite.Nil(err)
}

func (suite *processorTestSuite) TestProcessorWorkerProcessFailed() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("", errors.New("test"))

	worker := NewProcessorWorker[string](mockProcessor)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"
	close(receiver)

	worker.SetReceiveChan(receiver)

	sender := make(chan string, 1)
	worker.SetSendChan(sender)

	ctx := context.Background()
	err := worker.Run(ctx)
	suite.NotNil(err)
}
