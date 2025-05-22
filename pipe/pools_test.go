package pipe

import (
	"context"
	"fmt"
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

	newReaderMock := func() reader[*models.Token] {
		mock := mocks.NewMockreader[*models.Token](t)
		var mockCounter int
		mock.EXPECT().Read().RunAndReturn(func() (*models.Token, error) {
			if mockCounter < testCount {
				mockCounter++
				time.Sleep(testDealy)
				return defaultToken(), nil
			}

			return nil, io.EOF
		})

		mock.EXPECT().Close()

		return mock
	}

	newProcessorMock := func() processor[*models.Token] {
		mock := mocks.NewMockprocessor[*models.Token](t)
		mock.EXPECT().Process(defaultToken()).Return(defaultToken(), nil)

		return mock
	}

	pool := NewReaderBackupPool[*models.Token](testParallel, newReaderMock, newProcessorMock)
	require.NotNil(t, pool)

	err := pool.Run(context.Background())
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

	newReaderMock := func() reader[*models.Token] {
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

		return mock
	}

	newProcessorMock := func() processor[*models.Token] {
		mock := mocks.NewMockprocessor[*models.Token](t)
		mock.EXPECT().Process(defaultToken()).Return(defaultToken(), nil)

		return mock
	}

	pool := NewReaderBackupPool[*models.Token](testParallel, newReaderMock, newProcessorMock)
	require.NotNil(t, pool)

	err := pool.Run(context.Background())
	require.ErrorIs(t, err, errTest)
}

func TestPools_RunNewWriterBackupPool(t *testing.T) {
	t.Parallel()

	var mockCounterWrite int
	newWriterMock := func() writer[*models.Token] {
		mock := mocks.NewMockwriter[*models.Token](t)

		mock.EXPECT().Write(defaultToken()).RunAndReturn(func(*models.Token) (int, error) {
			mockCounterWrite++
			return testSize, nil
		})

		mock.EXPECT().Close().Return(nil)

		return mock
	}

	pool := NewWriterBackupPool[*models.Token](testParallel, newWriterMock, nil)
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

	err := pool.Run(context.Background())
	require.NoError(t, err)

	require.Equal(t, testCount*testParallel, mockCounterWrite)
}

func TestPools_RunNewWriterBackupPoolError(t *testing.T) {
	t.Parallel()

	newWriterMock := func() writer[*models.Token] {
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

		return mock
	}

	pool := NewWriterBackupPool[*models.Token](testParallel, newWriterMock, nil)
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

	err := pool.Run(context.Background())
	fmt.Println("ERROR:", err)
	require.ErrorIs(t, err, errTest)
}
