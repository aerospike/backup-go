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

package pipe

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const testParallel = 10

func TestPools_RunReaderBackupPool(t *testing.T) {
	t.Parallel()

	readMock := mocks.NewMockReader[*models.Token](t)
	var (
		mockCounter  int
		counterMutex sync.Mutex
	)

	ctx := context.Background()

	readMock.EXPECT().Read(mock.Anything).RunAndReturn(func(context.Context) (*models.Token, error) {
		counterMutex.Lock()
		currentCount := mockCounter
		if currentCount < testCount*testParallel {
			mockCounter++
			counterMutex.Unlock()
			time.Sleep(testDealy)
			return testToken(), nil
		}
		counterMutex.Unlock()

		return nil, io.EOF
	})

	readMock.EXPECT().Close()

	newProcessorMock := func() Processor[*models.Token] {
		mock := mocks.NewMockProcessor[*models.Token](t)
		mock.EXPECT().Process(testToken()).Return(testToken(), nil)

		return mock
	}

	pool := NewReaderPool[*models.Token]([]Reader[*models.Token]{readMock, readMock, readMock}, newProcessorMock)
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

	readMock := mocks.NewMockReader[*models.Token](t)
	var (
		mockCounter  int
		counterMutex sync.Mutex
	)

	ctx := context.Background()

	readMock.EXPECT().Read(mock.Anything).RunAndReturn(func(context.Context) (*models.Token, error) {
		counterMutex.Lock()
		currentCount := mockCounter
		if currentCount < testCount {
			mockCounter++
			counterMutex.Unlock()
			time.Sleep(testDealy)
			return testToken(), nil
		}
		counterMutex.Unlock()

		return nil, errTest
	})

	readMock.EXPECT().Close()

	newProcessorMock := func() Processor[*models.Token] {
		mock := mocks.NewMockProcessor[*models.Token](t)
		mock.EXPECT().Process(testToken()).Return(testToken(), nil)

		return mock
	}

	pool := NewReaderPool[*models.Token]([]Reader[*models.Token]{readMock, readMock, readMock}, newProcessorMock)
	require.NotNil(t, pool)

	err := pool.Run(ctx)
	require.ErrorIs(t, err, errTest)
}

func TestPools_RunNewWriterBackupPool(t *testing.T) {
	t.Parallel()

	writeMock := mocks.NewMockWriter[*models.Token](t)
	var (
		mockCounterWrite int
		writeMutex       sync.Mutex
	)

	writeMock.EXPECT().Write(mock.Anything, testToken()).RunAndReturn(func(context.Context, *models.Token) (int, error) {
		writeMutex.Lock()
		mockCounterWrite++
		writeMutex.Unlock()
		return testSize, nil
	})

	writeMock.EXPECT().Close().Return(nil)

	writers := make([]Writer[*models.Token], testParallel)
	for i := range testParallel {
		writers[i] = writeMock
	}

	pool := NewWriterPool[*models.Token](writers, nil)
	require.NotNil(t, pool)

	go func() {
		for range testCount {
			for i := range pool.Inputs {
				time.Sleep(testDealy)
				pool.Inputs[i] <- testToken()
			}
		}

		for i := range pool.Inputs {
			close(pool.Inputs[i])
		}
	}()

	ctx := context.Background()

	err := pool.Run(ctx)
	require.NoError(t, err)

	require.Equal(t, testCount*testParallel, mockCounterWrite)
}

func TestPools_RunNewWriterBackupPoolError(t *testing.T) {
	t.Parallel()

	writeMock := mocks.NewMockWriter[*models.Token](t)
	var (
		mockCounter  int
		counterMutex sync.Mutex
	)

	writeMock.EXPECT().Write(mock.Anything, testToken()).RunAndReturn(func(context.Context, *models.Token) (int, error) {
		counterMutex.Lock()
		currentCount := mockCounter
		if currentCount < testCount {
			mockCounter++
			counterMutex.Unlock()
			time.Sleep(testDealy)
			return testSize, nil
		}
		counterMutex.Unlock()

		return 0, errTest
	})

	writeMock.EXPECT().Close().Return(nil)

	pool := NewWriterPool[*models.Token]([]Writer[*models.Token]{writeMock, writeMock, writeMock}, nil)
	require.NotNil(t, pool)

	go func() {
		for range testCount * 2 {
			for i := range pool.Inputs {
				time.Sleep(testDealy)
				pool.Inputs[i] <- testToken()
			}
		}

		for i := range pool.Inputs {
			close(pool.Inputs[i])
		}
	}()

	ctx := context.Background()

	err := pool.Run(ctx)
	require.ErrorIs(t, err, errTest)
}
