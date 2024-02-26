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

package backuplib

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aerospike/aerospike-tools-backup-lib/mocks"
	"github.com/aerospike/aerospike-tools-backup-lib/models"
	"github.com/stretchr/testify/suite"
)

type proccessorTestSuite struct {
	suite.Suite
}

func (suite *proccessorTestSuite) TestProcessorWorker() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("test", nil)

	worker := NewProcessorWorker(mockProcessor)
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

func (suite *proccessorTestSuite) TestProcessorWorkerCancelOnReceive() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("test", nil)

	worker := NewProcessorWorker(mockProcessor)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"

	worker.SetReceiveChan(receiver)

	sender := make(chan string, 1)
	worker.SetSendChan(sender)

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	errors := make(chan error, 1)
	go func() {
		defer wg.Done()
		err := worker.Run(ctx)
		errors <- err
	}()

	// give the worker some time to start
	time.Sleep(100 * time.Millisecond)

	cancel()
	wg.Wait()

	err := <-errors
	suite.NotNil(err)

	data := <-sender
	suite.Equal("test", data)
}

func (suite *proccessorTestSuite) TestProcessorWorkerCancelOnSend() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("test", nil)

	worker := NewProcessorWorker(mockProcessor)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"

	worker.SetReceiveChan(receiver)

	sender := make(chan string)
	worker.SetSendChan(sender)

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	errors := make(chan error, 1)
	go func() {
		defer wg.Done()
		err := worker.Run(ctx)
		errors <- err
	}()

	// give the worker some time to start
	time.Sleep(100 * time.Millisecond)

	cancel()
	wg.Wait()

	err := <-errors
	suite.NotNil(err)
}

func (suite *proccessorTestSuite) TestProcessorWorkerReceiveClosed() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("test", nil)

	worker := NewProcessorWorker(mockProcessor)
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

func (suite *proccessorTestSuite) TestProcessorWorkerProcessFailed() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("", errors.New("test"))

	worker := NewProcessorWorker(mockProcessor)
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

func (suite *proccessorTestSuite) TestNOOPProcessor() {
	noop := NewNOOPProcessor()
	suite.NotNil(noop)

	data := &token{
		Type:   tokenTypeRecord,
		Record: &models.Record{},
	}
	processed, err := noop.Process(data)
	suite.Nil(err)
	suite.Equal(data, processed)
}

func TestProcessors(t *testing.T) {
	suite.Run(t, new(proccessorTestSuite))
}
