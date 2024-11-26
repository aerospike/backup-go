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
	"fmt"
	"testing"

	"github.com/aerospike/backup-go/pipeline/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type pipelineTestSuite struct {
	suite.Suite
}

func (suite *pipelineTestSuite) TestNewDataPipeline() {
	w1 := mocks.NewMockWorker[string](suite.T())
	w1.EXPECT().SetReceiveChan(mock.Anything)
	w1.EXPECT().SetSendChan(mock.Anything)
	w2 := mocks.NewMockWorker[string](suite.T())
	w2.EXPECT().SetReceiveChan(mock.Anything)
	w2.EXPECT().SetSendChan(mock.Anything)
	w3 := mocks.NewMockWorker[string](suite.T())
	w3.EXPECT().SetReceiveChan(mock.Anything)
	w3.EXPECT().SetSendChan(mock.Anything)

	workers := [][]Worker[string]{{w1, w2}, {w3}}
	routes := newRoutesForWorkers(len(workers))

	pipeline, err := NewPipeline(routes, workers...)
	suite.Require().Nil(err)
	suite.NotNil(pipeline)
}

func (suite *pipelineTestSuite) TestDataPipelineRun() {
	w1 := mocks.NewMockWorker[string](suite.T())
	w1.EXPECT().SetReceiveChan(mock.Anything)
	w1.EXPECT().SetSendChan(mock.Anything)
	w1.EXPECT().Run(mock.Anything).Return(nil)

	w2 := mocks.NewMockWorker[string](suite.T())
	w2.EXPECT().SetReceiveChan(mock.Anything)
	w2.EXPECT().SetSendChan(mock.Anything)
	w2.EXPECT().Run(mock.Anything).Return(nil)

	w3 := mocks.NewMockWorker[string](suite.T())
	w3.EXPECT().SetReceiveChan(mock.Anything)
	w3.EXPECT().SetSendChan(mock.Anything)
	w3.EXPECT().Run(mock.Anything).Return(nil)

	workers := [][]Worker[string]{{w1, w2}, {w3}}

	routes := newRoutesForWorkers(len(workers))
	pipeline, err := NewPipeline(routes, workers...)
	suite.Require().Nil(err)
	suite.NotNil(pipeline)

	ctx := context.Background()
	err = pipeline.Run(ctx)
	suite.Nil(err)
}

type mockWorker struct {
	receive <-chan string
	send    chan<- string
	mocks.MockWorker[string]
	// Means that it is the first worker who actively sends smth.
	active bool
}

var _ Worker[string] = (*mockWorker)(nil)

func newMockWorker(t *testing.T, active bool) *mockWorker {
	t.Helper()
	return &mockWorker{
		MockWorker: *mocks.NewMockWorker[string](t),
		active:     active,
	}
}

func (w *mockWorker) SetReceiveChan(c <-chan string) {
	w.receive = c
}

func (w *mockWorker) SetSendChan(c chan<- string) {
	w.send = c
}

func (w *mockWorker) Run(ctx context.Context) error {
	if w.active {
		for i := 0; i < 3; i++ {
			w.send <- fmt.Sprintf("%d", i)
		}
	} else {
		for {
			select {
			case <-ctx.Done():
				return nil
			case msg, active := <-w.receive:
				if !active {
					return nil
				}
				w.send <- msg
			}
		}
	}
	return nil
}

func (suite *pipelineTestSuite) TestDataPipelineRunWithChannels() {
	ctx := context.Background()

	w1 := newMockWorker(suite.T(), true)
	w1.EXPECT().SetReceiveChan(mock.Anything)
	w1.EXPECT().SetSendChan(mock.Anything)
	w1.EXPECT().Run(ctx)

	w2 := newMockWorker(suite.T(), false)
	w2.EXPECT().SetReceiveChan(mock.Anything)
	w2.EXPECT().SetSendChan(mock.Anything)
	w2.EXPECT().Run(ctx)

	w3 := newMockWorker(suite.T(), false)
	w3.EXPECT().SetReceiveChan(mock.Anything)
	w3.EXPECT().SetSendChan(mock.Anything)
	w3.EXPECT().Run(ctx)

	w4 := newMockWorker(suite.T(), false)
	w4.EXPECT().SetReceiveChan(mock.Anything)
	w4.EXPECT().SetSendChan(mock.Anything)
	w4.EXPECT().Run(ctx)

	workers := [][]Worker[string]{{w1}, {w3}, {w4, w2}}
	routes := newRoutesForWorkers(len(workers))
	pipeline, err := NewPipeline(routes, workers...)
	suite.Require().Nil(err)
	suite.NotNil(pipeline)

	err = pipeline.Run(ctx)
	suite.Nil(err)
}

func (suite *pipelineTestSuite) TestDataPipelineRunWorkerFails() {
	w1 := mocks.NewMockWorker[string](suite.T())
	w1.EXPECT().SetReceiveChan(mock.Anything)
	w1.EXPECT().SetSendChan(mock.Anything)
	w1.EXPECT().Run(mock.Anything).Return(nil)

	w2 := mocks.NewMockWorker[string](suite.T())
	w2.EXPECT().SetReceiveChan(mock.Anything)
	w2.EXPECT().SetSendChan(mock.Anything)
	w2.EXPECT().Run(mock.Anything).Return(nil)

	w3 := mocks.NewMockWorker[string](suite.T())
	w3.EXPECT().SetReceiveChan(mock.Anything)
	w3.EXPECT().SetSendChan(mock.Anything)
	w3.EXPECT().Run(mock.Anything).Return(errors.New("error"))

	w4 := mocks.NewMockWorker[string](suite.T())
	w4.EXPECT().SetReceiveChan(mock.Anything)
	w4.EXPECT().SetSendChan(mock.Anything)
	w4.EXPECT().Run(mock.Anything).Return(nil)

	workers := [][]Worker[string]{{w1, w2}, {w3}, {w4}}
	routes := newRoutesForWorkers(len(workers))
	pipeline, err := NewPipeline(routes, workers...)
	suite.Require().Nil(err)
	suite.NotNil(pipeline)

	ctx := context.Background()
	err = pipeline.Run(ctx)
	suite.NotNil(err)
}

func TestPipelineTestSuite(t *testing.T) {
	suite.Run(t, new(pipelineTestSuite))
}

func newRoutesForWorkers(n int) []Route[string] {
	result := make([]Route[string], 0, n)
	for i := 0; i < n; i++ {
		var r Route[string]
		r.output = NewRouteRuleSingle[string](channelSize, nil)
		r.input = NewRouteRuleSingle[string](channelSize, nil)

		result = append(result, r)
	}

	return result
}
