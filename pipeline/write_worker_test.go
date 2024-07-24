//go:build test
// +build test

package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/aerospike/backup-go/pipeline/mocks"
	"github.com/stretchr/testify/suite"
)

type writersTestSuite struct {
	suite.Suite
}

func TestWriters(t *testing.T) {
	suite.Run(t, new(writersTestSuite))
}

func (suite *writersTestSuite) TestWriteWorker() {
	mockWriter := mocks.NewMockDataWriter[string](suite.T())
	mockWriter.EXPECT().Write("test").Return(1, nil)
	mockWriter.EXPECT().Close().Return(nil)

	worker := NewWriteWorker[string](mockWriter, nil)
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
	mockWriter := mocks.NewMockDataWriter[string](suite.T())
	mockWriter.EXPECT().Close().Return(nil)

	worker := NewWriteWorker[string](mockWriter, nil)
	suite.NotNil(worker)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := worker.Run(ctx)
	suite.NotNil(err)
}

func (suite *writersTestSuite) TestWriteWorkerWriteFailed() {
	mockWriter := mocks.NewMockDataWriter[string](suite.T())
	mockWriter.EXPECT().Write("test").Return(0, errors.New("error"))
	mockWriter.EXPECT().Close().Return(nil)

	worker := NewWriteWorker[string](mockWriter, nil)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"
	close(receiver)

	worker.SetReceiveChan(receiver)

	ctx := context.Background()
	err := worker.Run(ctx)
	suite.NotNil(err)
}
