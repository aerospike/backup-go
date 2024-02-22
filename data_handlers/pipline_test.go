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

package datahandlers

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aerospike/aerospike-tools-backup-lib/data_handlers/mocks"

	"github.com/stretchr/testify/suite"
)

type pipelineTestSuite struct {
	suite.Suite
}

func (suite *pipelineTestSuite) TestNewDataPipeline() {
	reader := mocks.NewReader(suite.T())
	processor := mocks.NewProcessor(suite.T())
	writer := mocks.NewWriter(suite.T())

	readers := []Reader{reader}
	processors := []Processor{processor}
	writers := []Writer{writer}

	pipeline := NewDataPipeline(readers, processors, writers)
	suite.NotNil(pipeline)
}

func (suite *pipelineTestSuite) TestNewDataPipelineNoArgs() {
	pipeline := NewDataPipeline(nil, nil, nil)
	suite.Nil(pipeline)
}

func (suite *pipelineTestSuite) TestDataPipelineRun() {
	reader := mocks.NewReader(suite.T())
	processor := mocks.NewProcessor(suite.T())
	writer := mocks.NewWriter(suite.T())

	readers := []Reader{reader}
	processors := []Processor{processor}
	writers := []Writer{writer}

	pipeline := NewDataPipeline(readers, processors, writers)
	suite.NotNil(pipeline)

	readCalls := 0
	reader.EXPECT().Read().RunAndReturn(func() (any, error) {
		readCalls++
		if readCalls <= 3 {
			return "hi", nil
		}
		return nil, io.EOF
	})
	reader.EXPECT().Cancel()

	processor.EXPECT().Process("hi").Return("hi", nil)

	writer.EXPECT().Write("hi").Return(nil)
	writer.EXPECT().Cancel()

	err := pipeline.Run()
	suite.Nil(err)
}

func (suite *pipelineTestSuite) TestDataPipelineReaderFails() {
	reader := mocks.NewReader(suite.T())
	processor := mocks.NewProcessor(suite.T())
	writer := mocks.NewWriter(suite.T())

	readers := []Reader{reader}
	processors := []Processor{processor}
	writers := []Writer{writer}

	pipeline := NewDataPipeline(readers, processors, writers)
	suite.NotNil(pipeline)

	readCalls := 0
	reader.EXPECT().Read().RunAndReturn(func() (any, error) {
		readCalls++
		if readCalls <= 3 {
			return "hi", nil
		}
		return nil, errors.New("error")
	})
	reader.EXPECT().Cancel()

	processor.EXPECT().Process("hi").Return("hi", nil)

	writer.EXPECT().Write("hi").Return(nil)
	writer.EXPECT().Cancel()

	err := pipeline.Run()
	suite.NotNil(err)
}

func (suite *pipelineTestSuite) TestDataPipelineProcessorFails() {
	reader := mocks.NewReader(suite.T())
	processor := mocks.NewProcessor(suite.T())
	writer := mocks.NewWriter(suite.T())

	readers := []Reader{reader}
	processors := []Processor{processor}
	writers := []Writer{writer}

	reader.EXPECT().Read().Return("hi", nil)
	reader.EXPECT().Cancel()

	processCalls := 0
	processor.EXPECT().Process("hi").RunAndReturn(func(any) (any, error) {
		processCalls++
		if processCalls <= 3 {
			return "hi", nil
		}
		return nil, errors.New("error")
	})

	writer.EXPECT().Write("hi").Return(nil)
	writer.EXPECT().Cancel()

	pipeline := NewDataPipeline(readers, processors, writers)
	suite.NotNil(pipeline)

	err := pipeline.Run()
	suite.NotNil(err)
}

func (suite *pipelineTestSuite) TestDataPipelineWriterFails() {
	reader := mocks.NewReader(suite.T())
	processor := mocks.NewProcessor(suite.T())
	writer := mocks.NewWriter(suite.T())

	readers := []Reader{reader}
	processors := []Processor{processor}
	writers := []Writer{writer}

	reader.EXPECT().Read().Return("hi", nil)
	reader.EXPECT().Cancel()

	processor.EXPECT().Process("hi").Return("hi", nil)

	writeCalls := 0
	writer.EXPECT().Write("hi").RunAndReturn(func(any) error {
		writeCalls++
		if writeCalls <= 3 {
			return nil
		}
		return errors.New("error")
	})
	writer.EXPECT().Cancel()

	pipeline := NewDataPipeline(readers, processors, writers)
	suite.NotNil(pipeline)

	err := pipeline.Run()
	suite.NotNil(err)
}

func (suite *pipelineTestSuite) TestReadStageRun() {
	ctx, cancel := context.WithCancel(context.Background())

	reader := mocks.NewReader(suite.T())
	reader.EXPECT().Read().Return("hi", nil)
	reader.EXPECT().Cancel()

	stage := readStage{
		r:    reader,
		send: make(chan any),
	}

	errChan := make(chan error)
	go func() {
		errChan <- stage.Run(ctx)
		close(errChan)
	}()
	cancel()

	err := <-errChan
	suite.Nil(err)
}

func (suite *pipelineTestSuite) TestReadStageRunEOF() {
	ctx := context.Background()
	reader := mocks.NewReader(suite.T())
	reader.EXPECT().Read().Return(nil, io.EOF)
	reader.EXPECT().Cancel()

	stage := readStage{
		r:    reader,
		send: make(chan any),
	}

	errChan := make(chan error)
	go func() {
		errChan <- stage.Run(ctx)
		close(errChan)
	}()

	err := <-errChan
	suite.Nil(err)
}

func (suite *pipelineTestSuite) TestReadStageError() {
	ctx := context.Background()
	reader := mocks.NewReader(suite.T())
	reader.EXPECT().Read().Return(nil, errors.New("error"))
	reader.EXPECT().Cancel()

	stage := readStage{
		r:    reader,
		send: make(chan any),
	}

	errChan := make(chan error)
	go func() {
		errChan <- stage.Run(ctx)
		close(errChan)
	}()

	err := <-errChan
	suite.NotNil(err)
}

func TestPipelineTestSuite(t *testing.T) {
	suite.Run(t, new(pipelineTestSuite))
}
