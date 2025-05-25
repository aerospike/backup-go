package pipe

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe/mocks"
	"github.com/stretchr/testify/require"
)

func TestPipe_RunBackupPipe(t *testing.T) {
	t.Parallel()

	readersMock := mocks.NewMockReader[*models.Token](t)
	var mockCounter int
	readersMock.EXPECT().Read().RunAndReturn(func() (*models.Token, error) {
		if mockCounter < testCount*testParallel {
			mockCounter++
			time.Sleep(testDealy)
			return defaultToken(), nil
		}

		return nil, io.EOF
	})

	readersMock.EXPECT().Close()

	newProcessorMock := func() Processor[*models.Token] {
		mock := mocks.NewMockProcessor[*models.Token](t)
		mock.EXPECT().Process(defaultToken()).Return(defaultToken(), nil)

		return mock
	}

	var mockCounterWrite int
	writersMocks := mocks.NewMockWriter[*models.Token](t)

	writersMocks.EXPECT().Write(defaultToken()).RunAndReturn(func(*models.Token) (int, error) {
		mockCounterWrite++
		return testSize, nil
	})

	writersMocks.EXPECT().Close().Return(nil)

	ctx := context.Background()

	readerSlice := make([]Reader[*models.Token], testParallel)
	for i := range testParallel {
		readerSlice[i] = readersMock
	}

	p, err := NewBackupPipe(
		newProcessorMock,
		readerSlice,
		[]Writer[*models.Token]{writersMocks, writersMocks, writersMocks},
		nil,
		RoundRobin,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, p)

	err = p.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, testCount*testParallel, mockCounterWrite)
}

func TestPipe_RunBackupPipeError(t *testing.T) {
	t.Parallel()

	readersMock := mocks.NewMockReader[*models.Token](t)
	var mockCounter int
	readersMock.EXPECT().Read().RunAndReturn(func() (*models.Token, error) {
		if mockCounter < testCount {
			mockCounter++
			time.Sleep(testDealy)
			return defaultToken(), nil
		}

		return nil, errTest
	})
	readersMock.EXPECT().Close()

	newProcessorMock := func() Processor[*models.Token] {
		mock := mocks.NewMockProcessor[*models.Token](t)
		mock.EXPECT().Process(defaultToken()).Return(defaultToken(), nil)

		return mock
	}

	writersMocks := mocks.NewMockWriter[*models.Token](t)
	var mockCounterWrite int
	writersMocks.EXPECT().Write(defaultToken()).RunAndReturn(func(*models.Token) (int, error) {
		if mockCounterWrite < testCount {
			mockCounter++
			time.Sleep(testDealy)
			return testSize, nil
		}

		return 0, errTest
	})
	writersMocks.EXPECT().Close().Return(nil)

	ctx := context.Background()

	p, err := NewBackupPipe(
		newProcessorMock,
		[]Reader[*models.Token]{readersMock},
		[]Writer[*models.Token]{writersMocks, writersMocks, writersMocks},
		nil,
		RoundRobin,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, p)

	err = p.Run(ctx)
	require.ErrorIs(t, err, errTest)
}
