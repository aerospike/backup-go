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
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe/mocks"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

const (
	testCount     = 5
	testSize      = 10
	testDealy     = 100 * time.Millisecond
	testLongDelay = 300 * time.Millisecond
	testLimit     = 1
)

var errTest = errors.New("test error")

func defaultToken() *models.Token {
	return &models.Token{
		Type:   models.TokenTypeRecord,
		Record: &models.Record{},
		Size:   testSize,
		Filter: nil,
	}
}

func TestChains_ReaderBackupChain(t *testing.T) {
	t.Parallel()

	readerMock := mocks.NewMockReader[*models.Token](t)

	var mockCounter int
	readerMock.EXPECT().Read().RunAndReturn(func() (*models.Token, error) {
		if mockCounter < testCount {
			mockCounter++
			time.Sleep(testDealy)
			return defaultToken(), nil
		}

		return nil, io.EOF
	})

	readerMock.EXPECT().Close()

	processorMock := mocks.NewMockProcessor[*models.Token](t)
	processorMock.EXPECT().Process(defaultToken()).Return(defaultToken(), nil)

	readChain, output := NewReaderChain[*models.Token](readerMock, processorMock)
	require.NotNil(t, readChain)
	require.NotNil(t, output)

	go func() {
		err := readChain.Run(context.Background())
		require.NoError(t, err)
	}()

	var resultCounter int
	for range output {
		resultCounter++
	}

	require.Equal(t, testCount, resultCounter)
}

func TestChains_ReaderBackupChainContextCancel(t *testing.T) {
	t.Parallel()

	readerMock := mocks.NewMockReader[*models.Token](t)

	readerMock.EXPECT().Read().RunAndReturn(func() (*models.Token, error) {
		time.Sleep(testDealy)
		return defaultToken(), nil
	})

	readerMock.EXPECT().Close()

	processorMock := mocks.NewMockProcessor[*models.Token](t)
	processorMock.EXPECT().Process(defaultToken()).Return(defaultToken(), nil)

	readChain, output := NewReaderChain[*models.Token](readerMock, processorMock)
	require.NotNil(t, readChain)
	require.NotNil(t, output)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(testLongDelay)
		cancel()
	}()

	go func() {
		err := readChain.Run(ctx)
		require.ErrorIs(t, err, context.Canceled)
	}()

	//nolint:revive // Read from output to avoid deadlock.
	for range output {
	}
}

func TestChains_ReaderBackupChainContextReaderError(t *testing.T) {
	t.Parallel()

	readerMock := mocks.NewMockReader[*models.Token](t)

	var mockCounter int
	readerMock.EXPECT().Read().RunAndReturn(func() (*models.Token, error) {
		if mockCounter < testCount {
			mockCounter++
			time.Sleep(testDealy)
			return defaultToken(), nil
		}

		return nil, errTest
	})

	readerMock.EXPECT().Close()

	processorMock := mocks.NewMockProcessor[*models.Token](t)
	processorMock.EXPECT().Process(defaultToken()).Return(defaultToken(), nil)

	readChain, output := NewReaderChain[*models.Token](readerMock, processorMock)
	require.NotNil(t, readChain)
	require.NotNil(t, output)

	go func() {
		err := readChain.Run(context.Background())
		require.ErrorIs(t, err, errTest)
	}()

	//nolint:revive // Read from output to avoid deadlock.
	for range output {
	}
}

func TestChains_ReaderBackupChainContextProcessorError(t *testing.T) {
	t.Parallel()

	readerMock := mocks.NewMockReader[*models.Token](t)

	readerMock.EXPECT().Read().RunAndReturn(func() (*models.Token, error) {
		time.Sleep(testDealy)
		return defaultToken(), nil
	})

	readerMock.EXPECT().Close()

	processorMock := mocks.NewMockProcessor[*models.Token](t)
	processorMock.EXPECT().Process(defaultToken()).Return(nil, errTest)

	readChain, output := NewReaderChain[*models.Token](readerMock, processorMock)
	require.NotNil(t, readChain)
	require.NotNil(t, output)

	go func() {
		err := readChain.Run(context.Background())
		require.ErrorIs(t, err, errTest)
	}()

	//nolint:revive // Read from output to avoid deadlock.
	for range output {
	}
}

func TestChains_ReaderBackupChainContextProcessorFiltered(t *testing.T) {
	t.Parallel()

	readerMock := mocks.NewMockReader[*models.Token](t)

	var mockCounterRead int
	readerMock.EXPECT().Read().RunAndReturn(func() (*models.Token, error) {
		if mockCounterRead < testCount*2 {
			mockCounterRead++
			time.Sleep(testDealy)
			return defaultToken(), nil
		}

		return nil, io.EOF
	})

	readerMock.EXPECT().Close()

	processorMock := mocks.NewMockProcessor[*models.Token](t)
	var mockCounterProc int
	processorMock.EXPECT().Process(defaultToken()).RunAndReturn(func(*models.Token) (*models.Token, error) {
		if mockCounterProc < testCount {
			mockCounterProc++
			time.Sleep(testDealy)
			return defaultToken(), models.ErrFilteredOut
		}

		return defaultToken(), nil
	})

	readChain, output := NewReaderChain[*models.Token](readerMock, processorMock)
	require.NotNil(t, readChain)
	require.NotNil(t, output)

	go func() {
		err := readChain.Run(context.Background())
		require.NoError(t, err)
	}()

	//nolint:revive // Read from output to avoid deadlock.
	for range output {
	}
}

func TestChains_WriterBackupChain(t *testing.T) {
	t.Parallel()

	writerMock := mocks.NewMockWriter[*models.Token](t)

	var mockCounterWrite int
	writerMock.EXPECT().Write(defaultToken()).RunAndReturn(func(*models.Token) (int, error) {
		mockCounterWrite++
		return testSize, nil
	})

	writerMock.EXPECT().Close().Return(nil)

	writeChain, input := NewWriterChain[*models.Token](writerMock, nil)
	require.NotNil(t, writeChain)
	require.NotNil(t, input)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := writeChain.Run(context.Background())
		require.NoError(t, err)
	}()

	go func() {
		defer wg.Done()
		for range testCount {
			time.Sleep(testDealy)
			input <- defaultToken()
		}

		close(input)
	}()

	wg.Wait()
	require.Equal(t, testCount, mockCounterWrite)
}

func TestChains_WriterBackupChainContextCancel(t *testing.T) {
	t.Parallel()

	writerMock := mocks.NewMockWriter[*models.Token](t)

	var mockCounterWrite int
	writerMock.EXPECT().Write(defaultToken()).RunAndReturn(func(*models.Token) (int, error) {
		mockCounterWrite++
		return testSize, nil
	})

	writerMock.EXPECT().Close().Return(nil)

	writeChain, input := NewWriterChain[*models.Token](writerMock, nil)
	require.NotNil(t, writeChain)
	require.NotNil(t, input)

	var wg sync.WaitGroup
	wg.Add(2)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(testLongDelay)
		cancel()
	}()

	go func() {
		defer wg.Done()
		err := writeChain.Run(ctx)
		require.ErrorIs(t, err, context.Canceled)
	}()

	go func() {
		defer wg.Done()
		for range testCount {
			time.Sleep(testDealy)
			input <- defaultToken()
		}
	}()

	wg.Wait()
}

func TestChains_WriterBackupChainWriterError(t *testing.T) {
	t.Parallel()

	writerMock := mocks.NewMockWriter[*models.Token](t)

	writerMock.EXPECT().Write(defaultToken()).RunAndReturn(func(*models.Token) (int, error) {
		return testSize, errTest
	})

	writerMock.EXPECT().Close().Return(nil)

	writeChain, input := NewWriterChain[*models.Token](writerMock, nil)
	require.NotNil(t, writeChain)
	require.NotNil(t, input)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := writeChain.Run(context.Background())
		require.ErrorIs(t, err, errTest)
	}()

	go func() {
		defer wg.Done()
		for range testCount {
			time.Sleep(testDealy)
			input <- defaultToken()
		}

		close(input)
	}()

	wg.Wait()
}

func TestChains_WriterBackupChainCloseError(t *testing.T) {
	t.Parallel()

	writerMock := mocks.NewMockWriter[*models.Token](t)

	writerMock.EXPECT().Write(defaultToken()).RunAndReturn(func(*models.Token) (int, error) {
		return testSize, nil
	})

	writerMock.EXPECT().Close().Return(errTest)

	writeChain, input := NewWriterChain[*models.Token](writerMock, nil)
	require.NotNil(t, writeChain)
	require.NotNil(t, input)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := writeChain.Run(context.Background())
		fmt.Println(err)
		require.ErrorIs(t, err, errTest)
	}()

	go func() {
		defer wg.Done()
		for range testCount {
			time.Sleep(testDealy)
			input <- defaultToken()
		}

		close(input)
	}()

	wg.Wait()
}

func TestChains_WriterBackupChainBothError(t *testing.T) {
	t.Parallel()

	writerMock := mocks.NewMockWriter[*models.Token](t)

	writerMock.EXPECT().Write(defaultToken()).RunAndReturn(func(*models.Token) (int, error) {
		return testSize, errTest
	})

	writerMock.EXPECT().Close().Return(errTest)

	writeChain, input := NewWriterChain[*models.Token](writerMock, nil)
	require.NotNil(t, writeChain)
	require.NotNil(t, input)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := writeChain.Run(context.Background())
		fmt.Println(err)
		require.ErrorIs(t, err, errTest)
		require.ErrorContains(t, err, "write error")
		require.ErrorContains(t, err, "close error")
	}()

	go func() {
		defer wg.Done()
		for range testCount {
			time.Sleep(testDealy)
			input <- defaultToken()
		}

		close(input)
	}()

	wg.Wait()
}

func TestChains_WriterBackupChainLimiterError(t *testing.T) {
	t.Parallel()

	writerMock := mocks.NewMockWriter[*models.Token](t)

	writerMock.EXPECT().Write(defaultToken()).RunAndReturn(func(*models.Token) (int, error) {
		return testSize, nil
	})

	writerMock.EXPECT().Close().Return(nil)

	limiter := rate.NewLimiter(rate.Limit(testLimit), testLimit)

	writeChain, input := NewWriterChain[*models.Token](writerMock, limiter)
	require.NotNil(t, writeChain)
	require.NotNil(t, input)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := writeChain.Run(context.Background())
		fmt.Println(err)
		require.Error(t, err)
		require.ErrorContains(t, err, "exceeds limiter's burst")
	}()

	go func() {
		defer wg.Done()
		for range testCount {
			time.Sleep(testDealy)
			input <- defaultToken()
		}

		close(input)
	}()

	wg.Wait()
}
