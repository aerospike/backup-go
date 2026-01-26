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

func TestPipe_RunBackupPipe(t *testing.T) {
	t.Parallel()

	readersMock := mocks.NewMockReader[*models.Token](t)
	var (
		mockCounter  int
		counterMutex sync.Mutex
	)
	readersMock.EXPECT().Read(mock.Anything).RunAndReturn(func(context.Context) (*models.Token, error) {
		counterMutex.Lock()
		currentCount := mockCounter
		if currentCount < testCount*testParallel {
			mockCounter++
			counterMutex.Unlock()
			time.Sleep(testDelay)
			return testToken(), nil
		}
		counterMutex.Unlock()

		return nil, io.EOF
	})

	readersMock.EXPECT().Close()

	newProcessorMock := func() Processor[*models.Token] {
		m := mocks.NewMockProcessor[*models.Token](t)
		m.EXPECT().Process(testToken()).Return(testToken(), nil)

		return m
	}

	writersMocks := mocks.NewMockWriter[*models.Token](t)
	var (
		mockCounterWrite int
		writeMutex       sync.Mutex
	)

	writersMocks.EXPECT().Write(testToken()).RunAndReturn(func(*models.Token) (int, error) {
		writeMutex.Lock()
		mockCounterWrite++
		writeMutex.Unlock()
		return testSize, nil
	})

	writersMocks.EXPECT().Close().Return(nil)

	readerSlice := make([]Reader[*models.Token], testParallel)
	for i := range testParallel {
		readerSlice[i] = readersMock
	}

	p, err := NewPipe(
		newProcessorMock,
		readerSlice,
		[]Writer[*models.Token]{writersMocks, writersMocks, writersMocks},
		nil,
		RoundRobin,
	)
	require.NoError(t, err)
	require.NotNil(t, p)

	err = p.Run(t.Context())
	require.NoError(t, err)
	require.Equal(t, testCount*testParallel, mockCounterWrite)

	in, out := p.GetMetrics()
	require.Equal(t, 0, in)
	require.Equal(t, 0, out)
}

func TestPipe_RunBackupPipeError(t *testing.T) {
	t.Parallel()

	readersMock := mocks.NewMockReader[*models.Token](t)
	var (
		mockCounter  int
		counterMutex sync.Mutex
	)
	readersMock.EXPECT().Read(mock.Anything).RunAndReturn(func(context.Context) (*models.Token, error) {
		counterMutex.Lock()
		currentCount := mockCounter
		if currentCount < testCount {
			mockCounter++
			counterMutex.Unlock()
			time.Sleep(testDelay)
			return testToken(), nil
		}
		counterMutex.Unlock()

		return nil, errTest
	})
	readersMock.EXPECT().Close()

	newProcessorMock := func() Processor[*models.Token] {
		m := mocks.NewMockProcessor[*models.Token](t)
		m.EXPECT().Process(testToken()).Return(testToken(), nil)

		return m
	}

	writersMocks := mocks.NewMockWriter[*models.Token](t)
	var mockCounterWrite int
	var writeMutex sync.Mutex
	writersMocks.EXPECT().Write(testToken()).RunAndReturn(func(*models.Token) (int, error) {
		writeMutex.Lock()
		currentCount := mockCounterWrite
		if currentCount < testCount {
			mockCounterWrite++
			counterMutex.Lock()
			mockCounter++
			counterMutex.Unlock()
			writeMutex.Unlock()
			time.Sleep(testDelay)
			return testSize, nil
		}
		writeMutex.Unlock()

		return 0, errTest
	})
	writersMocks.EXPECT().Close().Return(nil)

	p, err := NewPipe(
		newProcessorMock,
		[]Reader[*models.Token]{readersMock, readersMock, readersMock},
		[]Writer[*models.Token]{writersMocks, writersMocks, writersMocks},
		nil,
		Fixed,
	)
	require.NoError(t, err)
	require.NotNil(t, p)

	err = p.Run(t.Context())
	require.ErrorIs(t, err, errTest)
}

func TestPipe_NewBackupPipeError(t *testing.T) {
	t.Parallel()

	newProcessorMock := func() Processor[*models.Token] {
		m := mocks.NewMockProcessor[*models.Token](t)
		m.EXPECT().Process(testToken()).Return(testToken(), nil)

		return m
	}

	p, err := NewPipe(
		newProcessorMock,
		nil,
		nil,
		nil,
		Split,
	)
	require.ErrorContains(t, err, "failed to create fanout")
	require.Nil(t, p)
}
