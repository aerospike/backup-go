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
	"github.com/stretchr/testify/assert"
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

	pipeline, err := NewPipeline(ModeSingle, nil, workers...)
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

	pipeline, err := NewPipeline(ModeSingle, nil, workers...)
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
	pipeline, err := NewPipeline(ModeSingle, nil, workers...)
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
	pipeline, err := NewPipeline(ModeSingle, nil, workers...)
	suite.Require().Nil(err)
	suite.NotNil(pipeline)

	ctx := context.Background()
	err = pipeline.Run(ctx)
	suite.NotNil(err)
}

func TestPipelineTestSuite(t *testing.T) {
	suite.Run(t, new(pipelineTestSuite))
}

func TestNewPipelineWithDifferentModes(t *testing.T) {
	t.Run("ModeSingle", func(t *testing.T) {
		worker1 := mocks.NewMockWorker[string](t)
		worker1.EXPECT().SetReceiveChan(mock.Anything)
		worker1.EXPECT().SetSendChan(mock.Anything)

		worker2 := mocks.NewMockWorker[string](t)
		worker2.EXPECT().SetReceiveChan(mock.Anything)
		worker2.EXPECT().SetSendChan(mock.Anything)

		workers := [][]Worker[string]{{worker1}, {worker2}}

		pipeline, err := NewPipeline(ModeSingle, nil, workers...)
		assert.NoError(t, err)
		assert.NotNil(t, pipeline)
	})

	t.Run("ModeParallel", func(t *testing.T) {
		worker1 := mocks.NewMockWorker[string](t)
		worker1.EXPECT().SetReceiveChan(mock.Anything)
		worker1.EXPECT().SetSendChan(mock.Anything)

		worker2 := mocks.NewMockWorker[string](t)
		worker2.EXPECT().SetReceiveChan(mock.Anything)
		worker2.EXPECT().SetSendChan(mock.Anything)

		workers := [][]Worker[string]{{worker1}, {worker2}}

		pipeline, err := NewPipeline(ModeParallel, nil, workers...)
		assert.NoError(t, err)
		assert.NotNil(t, pipeline)
	})

	t.Run("ModeSingleParallel", func(t *testing.T) {
		worker1 := mocks.NewMockWorker[string](t)
		worker1.EXPECT().SetReceiveChan(mock.Anything)
		worker1.EXPECT().SetSendChan(mock.Anything)

		worker2 := mocks.NewMockWorker[string](t)
		worker2.EXPECT().SetReceiveChan(mock.Anything)
		worker2.EXPECT().SetSendChan(mock.Anything)

		worker3 := mocks.NewMockWorker[string](t)
		worker3.EXPECT().SetReceiveChan(mock.Anything)
		worker3.EXPECT().SetSendChan(mock.Anything)

		sf := func(v string) int {
			if v == "test" {
				return 0
			}
			return 1
		}

		workers := [][]Worker[string]{{worker1}, {worker2}, {worker3}}

		pipeline, err := NewPipeline(ModeSingleParallel, sf, workers...)
		assert.NoError(t, err)
		assert.NotNil(t, pipeline)
	})

	t.Run("ModeSingleParallelWrongStagesNumber", func(t *testing.T) {
		worker1 := mocks.NewMockWorker[string](t)
		worker2 := mocks.NewMockWorker[string](t)

		// Split function for ModeSingleParallel
		sf := func(_ string) int {
			return 0
		}

		workers := [][]Worker[string]{{worker1}, {worker2}}

		pipeline, err := NewPipeline(ModeSingleParallel, sf, workers...)
		assert.Error(t, err)
		assert.Nil(t, pipeline)
		assert.Contains(t, err.Error(), "this pipeline mode supports only 3 working groups")
	})

	t.Run("NoWorkers", func(t *testing.T) {
		pipeline, err := NewPipeline[string](ModeSingle, nil)
		assert.Error(t, err)
		assert.Nil(t, pipeline)
		assert.Contains(t, err.Error(), "workGroups is empty")
	})
}
