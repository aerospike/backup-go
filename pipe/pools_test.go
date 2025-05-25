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

const testParallel = 10

func TestPools_RunReaderBackupPool(t *testing.T) {
	t.Parallel()

	mock := mocks.NewMockreader[*models.Token](t)
	var mockCounter int
	mock.EXPECT().Read().RunAndReturn(func() (*models.Token, error) {
		if mockCounter < testCount*testParallel {
			mockCounter++
			time.Sleep(testDealy)
			return defaultToken(), nil
		}

		return nil, io.EOF
	})

	mock.EXPECT().Close()

	newProcessorMock := func() Processor[*models.Token] {
		mock := mocks.NewMockprocessor[*models.Token](t)
		mock.EXPECT().Process(defaultToken()).Return(defaultToken(), nil)

		return mock
	}

	ctx := context.Background()

	pool := NewReaderBackupPool[*models.Token]([]Reader[*models.Token]{mock, mock, mock}, newProcessorMock)
	require.NotNil(t, pool)

	err := pool.Run(ctx)
	require.NoError(t, err)

	var resultCounter int
	for i := range pool.Outputs {
		for range pool.Outputs[i] {
			resultCounter++
		}
	}

	require.Equal(t, testCount*testParallel, resultCounter)
}

func TestPools_RunReaderBackupPoolError(t *testing.T) {
	t.Parallel()

	mock := mocks.NewMockreader[*models.Token](t)
	var mockCounter int
	mock.EXPECT().Read().RunAndReturn(func() (*models.Token, error) {
		if mockCounter < testCount {
			mockCounter++
			time.Sleep(testDealy)
			return defaultToken(), nil
		}

		return nil, errTest
	})

	mock.EXPECT().Close()

	newProcessorMock := func() Processor[*models.Token] {
		mock := mocks.NewMockprocessor[*models.Token](t)
		mock.EXPECT().Process(defaultToken()).Return(defaultToken(), nil)

		return mock
	}

	ctx := context.Background()

	pool := NewReaderBackupPool[*models.Token]([]Reader[*models.Token]{mock, mock, mock}, newProcessorMock)
	require.NotNil(t, pool)

	err := pool.Run(ctx)
	require.ErrorIs(t, err, errTest)
}

func TestPools_RunNewWriterBackupPool(t *testing.T) {
	t.Parallel()

	var mockCounterWrite int
	mock := mocks.NewMockwriter[*models.Token](t)

	mock.EXPECT().Write(defaultToken()).RunAndReturn(func(*models.Token) (int, error) {
		mockCounterWrite++
		return testSize, nil
	})

	mock.EXPECT().Close().Return(nil)

	ctx := context.Background()

	writers := make([]Writer[*models.Token], testParallel)
	for i := range testParallel {
		writers[i] = mock
	}

	pool := NewWriterBackupPool[*models.Token](writers, nil)
	require.NotNil(t, pool)

	go func() {
		for range testCount {
			for i := range pool.Inputs {
				time.Sleep(testDealy)
				pool.Inputs[i] <- defaultToken()
			}
		}

		for i := range pool.Inputs {
			close(pool.Inputs[i])
		}
	}()

	err := pool.Run(ctx)
	require.NoError(t, err)

	require.Equal(t, testCount*testParallel, mockCounterWrite)
}

func TestPools_RunNewWriterBackupPoolError(t *testing.T) {
	t.Parallel()

	mock := mocks.NewMockwriter[*models.Token](t)

	var mockCounter int
	mock.EXPECT().Write(defaultToken()).RunAndReturn(func(*models.Token) (int, error) {
		if mockCounter < testCount {
			mockCounter++
			time.Sleep(testDealy)
			return testSize, nil
		}

		return 0, errTest
	})

	mock.EXPECT().Close().Return(nil)

	ctx := context.Background()

	pool := NewWriterBackupPool[*models.Token]([]Writer[*models.Token]{mock, mock, mock}, nil)
	require.NotNil(t, pool)

	go func() {
		for range testCount * 2 {
			for i := range pool.Inputs {
				time.Sleep(testDealy)
				pool.Inputs[i] <- defaultToken()
			}
		}

		for i := range pool.Inputs {
			close(pool.Inputs[i])
		}
	}()

	err := pool.Run(ctx)
	require.ErrorIs(t, err, errTest)
}
