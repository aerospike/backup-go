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
	mockReader := mocks.NewDataReader[string](suite.T())

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
	mockReader := mocks.NewDataReader[string](suite.T())
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
