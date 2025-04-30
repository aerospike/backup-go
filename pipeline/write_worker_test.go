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
	"errors"
	"testing"

	"github.com/aerospike/backup-go/pipeline/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type writersTestSuite struct {
	suite.Suite
}

func TestWriters(t *testing.T) {
	suite.Run(t, new(writersTestSuite))
}

func (suite *writersTestSuite) TestWriteWorker() {
	mock := mocks.NewMockDataWriter[string](suite.T())
	mock.EXPECT().Write("test").Return(1, nil)
	mock.EXPECT().Close().Return(nil)

	worker := NewWriteWorker[string](mock, nil)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"
	close(receiver)

	worker.SetReceiveChan(receiver)

	ctx := context.Background()
	err := worker.Run(ctx)
	suite.Nil(err)
}

func (suite *writersTestSuite) TestWriteWorkerClose() {
	mock := mocks.NewMockDataWriter[string](suite.T())
	mock.EXPECT().Close().Return(nil)

	worker := NewWriteWorker[string](mock, nil)
	suite.NotNil(worker)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := worker.Run(ctx)
	suite.NotNil(err)
}

func (suite *writersTestSuite) TestWriteWorkerWriteFailed() {
	mock := mocks.NewMockDataWriter[string](suite.T())
	mock.EXPECT().Write("test").Return(0, errors.New("error"))
	mock.EXPECT().Close().Return(nil)

	worker := NewWriteWorker[string](mock, nil)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"
	close(receiver)

	worker.SetReceiveChan(receiver)

	ctx := context.Background()
	err := worker.Run(ctx)
	suite.NotNil(err)
}

func TestReadWorkerSetReceiveChan(t *testing.T) {
	mock := &mockReader[string]{}

	worker := &readWorker[string]{
		reader: mock,
	}

	receiveChan := make(chan string, 1)

	worker.SetReceiveChan(receiveChan)

	if worker.reader != mock {
		t.Error("SetReceiveChan modified the reader field")
	}
}

// mockReader is a simple implementation of dataReader for testing
type mockReader[T any] struct{}

func (r *mockReader[T]) Read() (T, error) {
	var zero T
	return zero, nil
}

func (r *mockReader[T]) Close() {
	// no-op
}

func TestWriteWorkerSetSendChan(t *testing.T) {
	mock := &mockWriter[string]{}

	receiveChan := make(chan string, 1)

	worker := &writeWorker[string]{
		DataWriter: mock,
		receive:    receiveChan,
	}

	sendChan := make(chan string, 1)

	worker.SetSendChan(sendChan)

	if worker.DataWriter != mock {
		t.Error("SetSendChan modified the DataWriter field")
	}
	if worker.receive != receiveChan {
		t.Error("SetSendChan modified the receive field")
	}
}

// mockWriter is a simple implementation of DataWriter for testing
type mockWriter[T any] struct{}

func (w *mockWriter[T]) Write(_ T) (int, error) {
	return 0, nil
}

func (w *mockWriter[T]) Close() error {
	return nil
}

func TestProcessorWorkerGetMetrics(t *testing.T) {
	receiveChan := make(chan string, 5)
	sendChan := make(chan string, 3)

	for i := 0; i < 2; i++ {
		receiveChan <- "test"
	}
	for i := 0; i < 1; i++ {
		sendChan <- "test"
	}

	worker := &ProcessorWorker[string]{}
	worker.SetReceiveChan(receiveChan)
	worker.SetSendChan(sendChan)

	in, out := worker.GetMetrics()
	assert.Equal(t, 2, in, "Expected 2 items in receive channel")
	assert.Equal(t, 1, out, "Expected 1 item in send channel")
}

func TestReadWorkerGetMetrics(t *testing.T) {
	sendChan := make(chan string, 3)

	for i := 0; i < 2; i++ {
		sendChan <- "test"
	}

	worker := &readWorker[string]{}
	worker.SetSendChan(sendChan)

	in, out := worker.GetMetrics()
	assert.Equal(t, 0, in, "Expected 0 items in receive channel (read workers don't have receive channels)")
	assert.Equal(t, 2, out, "Expected 2 items in send channel")
}

func TestWriteWorkerGetMetrics(t *testing.T) {
	receiveChan := make(chan string, 5)

	for i := 0; i < 3; i++ {
		receiveChan <- "test"
	}

	worker := &writeWorker[string]{}
	worker.SetReceiveChan(receiveChan)

	in, out := worker.GetMetrics()
	assert.Equal(t, 3, in, "Expected 3 items in receive channel")
	assert.Equal(t, 0, out, "Expected 0 items in send channel (write workers don't have send channels)")
}

func TestStageGetMetrics(t *testing.T) {
	worker1 := &testWorker{
		receiveChannel: make(chan string, 2),
		sendChannel:    make(chan string, 3),
	}
	worker2 := &testWorker{
		receiveChannel: make(chan string, 4),
		sendChannel:    make(chan string, 2),
	}

	worker1.receiveChannel <- "test1"
	worker1.receiveChannel <- "test2"
	worker1.sendChannel <- "test1"

	worker2.receiveChannel <- "test1"
	worker2.receiveChannel <- "test2"
	worker2.receiveChannel <- "test3"
	worker2.sendChannel <- "test1"
	worker2.sendChannel <- "test2"

	route := Route[string]{
		input:  NewRouteRuleParallel[string](256, nil),
		output: NewRouteRuleParallel[string](256, nil),
	}

	stage := newStage(route, worker1, worker2)

	// Test GetMetrics
	// In parallel mode, metrics are summed for all workers:
	// - First worker: 2 in, 1 out
	// - Second worker: 3 in, 2 out
	// - For parallel mode, we add both workers' metrics
	in, out := stage.GetMetrics()
	assert.Equal(t, 7, in, "Expected 7 items in receive channels (2 + 2 + 3)")
	assert.Equal(t, 4, out, "Expected 4 items in send channels (1 + 1 + 2)")

	routeSingle := Route[string]{
		input:  NewRouteRuleSingle[string](256, nil),
		output: NewRouteRuleSingle[string](256, nil),
	}

	stageSingle := newStage(routeSingle, worker1, worker2)

	inSingle, outSingle := stageSingle.GetMetrics()
	assert.Equal(t, 2, inSingle, "Expected 2 items in receive channels (only first worker)")
	assert.Equal(t, 1, outSingle, "Expected 1 item in send channels (only first worker)")
}

// testWorker is a simple implementation of Worker for testing GetMetrics
type testWorker struct {
	receiveChannel chan string
	sendChannel    chan string
}

func (w *testWorker) SetReceiveChan(_ <-chan string) {
	// No-op for testing
}

func (w *testWorker) SetSendChan(_ chan<- string) {
	// No-op for testing
}

func (w *testWorker) Run(_ context.Context) error {
	return nil
}

func (w *testWorker) GetMetrics() (in, out int) {
	return len(w.receiveChannel), len(w.sendChannel)
}

func TestPipelineGetMetrics(t *testing.T) {
	worker1 := &testWorker{
		receiveChannel: make(chan string, 2),
		sendChannel:    make(chan string, 3),
	}
	worker2 := &testWorker{
		receiveChannel: make(chan string, 4),
		sendChannel:    make(chan string, 2),
	}

	worker1.receiveChannel <- "test1"
	worker1.receiveChannel <- "test2"
	worker1.sendChannel <- "test1"

	worker2.receiveChannel <- "test1"
	worker2.receiveChannel <- "test2"
	worker2.receiveChannel <- "test3"
	worker2.sendChannel <- "test1"
	worker2.sendChannel <- "test2"

	workers := [][]Worker[string]{{worker1}, {worker2}}
	pipeline, err := NewPipeline(ModeSingle, nil, workers...)
	assert.NoError(t, err)

	// Test GetMetrics
	// Pipeline.GetMetrics returns:
	// - First stage's output as the pipeline's input
	// - Last stage's input as the pipeline's output
	in, out := pipeline.GetMetrics()
	assert.Equal(t, 1, in, "Expected 1 item from first stage's output")
	assert.Equal(t, 3, out, "Expected 3 items from last stage's input")

	pipelineParallel, err := NewPipeline(ModeParallel, nil, workers...)
	assert.NoError(t, err)

	inParallel, outParallel := pipelineParallel.GetMetrics()
	assert.Equal(t, 2, inParallel, "Expected 2 items from first stage's output in parallel mode")
	assert.Equal(t, 6, outParallel, "Expected 6 items from last stage's input in parallel mode")
}
