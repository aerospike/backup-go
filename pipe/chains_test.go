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
	"io"
	"testing"
	"time"

	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

const (
	testCount     = 5
	testSize      = 10
	testDelay     = 100 * time.Millisecond
	testLongDelay = 300 * time.Millisecond
)

var errTest = errors.New("test error")

func testToken() *models.Token {
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
	readerMock.EXPECT().Read(mock.Anything).RunAndReturn(func(context.Context) (*models.Token, error) {
		if mockCounter < testCount {
			mockCounter++
			time.Sleep(testDelay)
			return testToken(), nil
		}

		return nil, io.EOF
	})

	readerMock.EXPECT().Close()

	processorMock := mocks.NewMockProcessor[*models.Token](t)
	processorMock.EXPECT().Process(testToken()).Return(testToken(), nil)

	readChain, output := NewReaderChain[*models.Token](readerMock, processorMock)
	require.NotNil(t, readChain)
	require.NotNil(t, output)

	g := &errgroup.Group{}

	g.Go(func() error {
		return readChain.Run(t.Context())
	})

	var resultCounter int
	for range output {
		resultCounter++
	}

	err := g.Wait()
	require.NoError(t, err)
	require.Equal(t, testCount, resultCounter)
}

func TestChains_ReaderBackupChainContextCancel(t *testing.T) {
	t.Parallel()

	readerMock := mocks.NewMockReader[*models.Token](t)
	ctx, cancel := context.WithCancel(t.Context())

	readerMock.EXPECT().Read(mock.Anything).RunAndReturn(func(context.Context) (*models.Token, error) {
		time.Sleep(testDelay)
		return testToken(), nil
	})

	readerMock.EXPECT().Close()

	processorMock := mocks.NewMockProcessor[*models.Token](t)
	processorMock.EXPECT().Process(testToken()).Return(testToken(), nil)

	readChain, output := NewReaderChain[*models.Token](readerMock, processorMock)
	require.NotNil(t, readChain)
	require.NotNil(t, output)

	go func() {
		time.Sleep(testLongDelay)
		cancel()
	}()

	g := &errgroup.Group{}
	g.Go(func() error {
		return readChain.Run(ctx)
	})

	for range output {
		_ = 0 // Read from output to avoid deadlock.
	}

	err := g.Wait()
	require.ErrorIs(t, err, context.Canceled)
}

func TestChains_ReaderBackupChainContextCancelSecond(t *testing.T) {
	t.Parallel()

	readerMock := mocks.NewMockReader[*models.Token](t)
	ctx, cancel := context.WithCancel(t.Context())

	var mockCounter int
	readerMock.EXPECT().Read(mock.Anything).RunAndReturn(func(context.Context) (*models.Token, error) {
		if mockCounter < testCount {
			mockCounter++
			time.Sleep(testDelay)
			return testToken(), nil
		}

		return nil, errTest
	})

	readerMock.EXPECT().Close()

	processorMock := mocks.NewMockProcessor[*models.Token](t)
	processorMock.EXPECT().Process(testToken()).Return(testToken(), nil)

	readChain, output := NewReaderChain[*models.Token](readerMock, processorMock)
	require.NotNil(t, readChain)
	require.NotNil(t, output)

	go func() {
		time.Sleep(testLongDelay)
		cancel()
	}()

	g := &errgroup.Group{}
	g.Go(func() error {
		return readChain.Run(ctx)
	})

	<-output

	err := g.Wait()
	require.ErrorIs(t, err, context.Canceled)
}

func TestChains_ReaderBackupChainContextReaderError(t *testing.T) {
	t.Parallel()

	readerMock := mocks.NewMockReader[*models.Token](t)

	var mockCounter int
	readerMock.EXPECT().Read(mock.Anything).RunAndReturn(func(context.Context) (*models.Token, error) {
		if mockCounter < testCount {
			mockCounter++
			time.Sleep(testDelay)
			return testToken(), nil
		}

		return nil, errTest
	})

	readerMock.EXPECT().Close()

	processorMock := mocks.NewMockProcessor[*models.Token](t)
	processorMock.EXPECT().Process(testToken()).Return(testToken(), nil)

	readChain, output := NewReaderChain[*models.Token](readerMock, processorMock)
	require.NotNil(t, readChain)
	require.NotNil(t, output)

	g := &errgroup.Group{}

	g.Go(func() error {
		return readChain.Run(t.Context())
	})

	for range output {
		_ = 0 // Read from output to avoid deadlock.
	}

	err := g.Wait()
	require.ErrorIs(t, err, errTest)
}

func TestChains_ReaderBackupChainContextProcessorError(t *testing.T) {
	t.Parallel()

	readerMock := mocks.NewMockReader[*models.Token](t)

	readerMock.EXPECT().Read(mock.Anything).RunAndReturn(func(context.Context) (*models.Token, error) {
		time.Sleep(testDelay)
		return testToken(), nil
	})

	readerMock.EXPECT().Close()

	processorMock := mocks.NewMockProcessor[*models.Token](t)
	processorMock.EXPECT().Process(testToken()).Return(nil, errTest)

	readChain, output := NewReaderChain[*models.Token](readerMock, processorMock)
	require.NotNil(t, readChain)
	require.NotNil(t, output)

	g := &errgroup.Group{}

	g.Go(func() error {
		return readChain.Run(t.Context())
	})

	for range output {
		_ = 0 // Read from output to avoid deadlock.
	}

	err := g.Wait()
	require.ErrorIs(t, err, errTest)
}

func TestChains_ReaderBackupChainContextProcessorFiltered(t *testing.T) {
	t.Parallel()

	readerMock := mocks.NewMockReader[*models.Token](t)

	var mockCounterRead int
	readerMock.EXPECT().Read(mock.Anything).RunAndReturn(func(context.Context) (*models.Token, error) {
		if mockCounterRead < testCount*2 {
			mockCounterRead++
			time.Sleep(testDelay)
			return testToken(), nil
		}

		return nil, io.EOF
	})

	readerMock.EXPECT().Close()

	processorMock := mocks.NewMockProcessor[*models.Token](t)
	var mockCounterProc int
	processorMock.EXPECT().Process(testToken()).RunAndReturn(func(*models.Token) (*models.Token, error) {
		if mockCounterProc < testCount {
			mockCounterProc++
			time.Sleep(testDelay)
			return testToken(), models.ErrFilteredOut
		}

		return testToken(), nil
	})

	readChain, output := NewReaderChain[*models.Token](readerMock, processorMock)
	require.NotNil(t, readChain)
	require.NotNil(t, output)

	g := &errgroup.Group{}

	g.Go(func() error {
		return readChain.Run(t.Context())
	})

	for range output {
		_ = 0 // Read from output to avoid deadlock.
	}

	err := g.Wait()
	require.NoError(t, err)
}

func TestChains_WriterBackupChain(t *testing.T) {
	t.Parallel()

	writerMock := mocks.NewMockWriter[*models.Token](t)

	var mockCounterWrite int
	writerMock.EXPECT().Write(testToken()).RunAndReturn(func(*models.Token) (int, error) {
		mockCounterWrite++
		return testSize, nil
	})

	writerMock.EXPECT().Close().Return(nil)

	writeChain, input := NewWriterChain[*models.Token](writerMock, nil)
	require.NotNil(t, writeChain)
	require.NotNil(t, input)

	g := &errgroup.Group{}

	g.Go(func() error {
		return writeChain.Run(t.Context())
	})

	g.Go(func() error {
		for range testCount {
			time.Sleep(testDelay)
			input <- testToken()
		}

		close(input)
		return nil
	})

	err := g.Wait()
	require.NoError(t, err)
	require.Equal(t, testCount, mockCounterWrite)
}

func TestChains_WriterBackupChainContextCancel(t *testing.T) {
	t.Parallel()

	writerMock := mocks.NewMockWriter[*models.Token](t)
	ctx, cancel := context.WithCancel(t.Context())

	var mockCounterWrite int
	writerMock.EXPECT().Write(testToken()).RunAndReturn(func(*models.Token) (int, error) {
		mockCounterWrite++
		return testSize, nil
	})

	writerMock.EXPECT().Close().Return(nil)

	writeChain, input := NewWriterChain[*models.Token](writerMock, nil)
	require.NotNil(t, writeChain)
	require.NotNil(t, input)

	go func() {
		time.Sleep(testLongDelay)
		cancel()
	}()

	g := &errgroup.Group{}
	g.Go(func() error {
		return writeChain.Run(ctx)
	})

	g.Go(func() error {
		for range testCount {
			time.Sleep(testDelay)
			input <- testToken()
		}
		return nil
	})

	err := g.Wait()
	require.ErrorIs(t, err, context.Canceled)
	require.Positive(t, mockCounterWrite)
}

func TestChains_WriterBackupChainWriterError(t *testing.T) {
	t.Parallel()

	writerMock := mocks.NewMockWriter[*models.Token](t)

	writerMock.EXPECT().Write(testToken()).RunAndReturn(func(*models.Token) (int, error) {
		return testSize, errTest
	})

	writerMock.EXPECT().Close().Return(nil)

	writeChain, input := NewWriterChain[*models.Token](writerMock, nil)
	require.NotNil(t, writeChain)
	require.NotNil(t, input)

	g := &errgroup.Group{}

	g.Go(func() error {
		return writeChain.Run(t.Context())
	})

	g.Go(func() error {
		for range testCount {
			time.Sleep(testDelay)
			input <- testToken()
		}

		close(input)
		return nil
	})

	err := g.Wait()
	require.ErrorIs(t, err, errTest)
}

func TestChains_WriterBackupChainCloseError(t *testing.T) {
	t.Parallel()

	writerMock := mocks.NewMockWriter[*models.Token](t)

	writerMock.EXPECT().Write(testToken()).RunAndReturn(func(*models.Token) (int, error) {
		return testSize, nil
	})

	writerMock.EXPECT().Close().Return(errTest)

	writeChain, input := NewWriterChain[*models.Token](writerMock, nil)
	require.NotNil(t, writeChain)
	require.NotNil(t, input)

	g := &errgroup.Group{}

	g.Go(func() error {
		err := writeChain.Run(t.Context())
		return err
	})

	g.Go(func() error {
		for range testCount {
			time.Sleep(testDelay)
			input <- testToken()
		}

		close(input)
		return nil
	})

	err := g.Wait()
	require.ErrorIs(t, err, errTest)
}

func TestChains_WriterBackupChainBothError(t *testing.T) {
	t.Parallel()

	writerMock := mocks.NewMockWriter[*models.Token](t)

	writerMock.EXPECT().Write(testToken()).RunAndReturn(func(*models.Token) (int, error) {
		return testSize, errTest
	})

	writerMock.EXPECT().Close().Return(errTest)

	writeChain, input := NewWriterChain[*models.Token](writerMock, nil)
	require.NotNil(t, writeChain)
	require.NotNil(t, input)

	g := &errgroup.Group{}

	g.Go(func() error {
		err := writeChain.Run(t.Context())
		return err
	})

	g.Go(func() error {
		for range testCount {
			time.Sleep(testDelay)
			input <- testToken()
		}

		close(input)
		return nil
	})

	err := g.Wait()
	require.ErrorIs(t, err, errTest)
	require.ErrorContains(t, err, "write error")
	require.ErrorContains(t, err, "close error")
}
