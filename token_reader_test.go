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

package backup

import (
	"io"
	"log/slog"
	"testing"

	"github.com/aerospike/backup-go/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockReadCloser is a mock implementation of io.ReadCloser
type MockReadCloser struct {
	mock.Mock
}

func (m *MockReadCloser) Read(p []byte) (n int, err error) {
	args := m.Called(p)
	return args.Int(0), args.Error(1)
}

func (m *MockReadCloser) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestTokenReader_ReadSingleToken(t *testing.T) {
	logger := slog.Default()
	readersCh := make(chan models.File, 1)
	mockReader := new(MockReadCloser)
	readersCh <- models.File{Reader: mockReader}

	mockDecoder := mocks.NewMockDecoder[*models.Token](t)
	mockDecoder.EXPECT().NextToken().Return(&models.Token{Type: models.TokenTypeRecord}, nil).Once()

	convertFn := func(io.ReadCloser, uint64, string) Decoder[*models.Token] {
		return mockDecoder
	}

	tr := newTokenReader(readersCh, logger, convertFn)

	token, err := tr.Read()
	assert.NoError(t, err)
	assert.NotNil(t, token)
	assert.Equal(t, models.TokenTypeRecord, token.Type)

	mockDecoder.AssertExpectations(t)
}

func TestTokenReader_ReadMultipleTokensFromSingleReader(t *testing.T) {
	logger := slog.Default()
	readersCh := make(chan models.File, 1)
	mockReader := new(MockReadCloser)
	readersCh <- models.File{Reader: mockReader}

	mockDecoder := mocks.NewMockDecoder[*models.Token](t)
	mockDecoder.EXPECT().NextToken().Return(&models.Token{Type: models.TokenTypeRecord}, nil).Times(3)
	mockDecoder.EXPECT().NextToken().Return((*models.Token)(nil), io.EOF).Once()

	convertFn := func(io.ReadCloser, uint64, string) Decoder[*models.Token] {
		return mockDecoder
	}

	tr := newTokenReader(readersCh, logger, convertFn)

	for i := 0; i < 3; i++ {
		token, err := tr.Read()
		assert.NoError(t, err)
		assert.NotNil(t, token)
		assert.Equal(t, models.TokenTypeRecord, token.Type)
	}

	close(readersCh)
	mockReader.On("Close").Return(nil).Once()

	_, err := tr.Read()
	assert.Equal(t, io.EOF, err)

	mockDecoder.AssertExpectations(t)
	mockReader.AssertExpectations(t)
}

func TestTokenReader_ReadFromMultipleReaders(t *testing.T) {
	logger := slog.Default()
	readersCh := make(chan models.File, 2)
	mockReader1 := new(MockReadCloser)
	mockReader2 := new(MockReadCloser)
	readersCh <- models.File{Reader: mockReader1}
	readersCh <- models.File{Reader: mockReader2}

	mockDecoder1 := mocks.NewMockDecoder[*models.Token](t)
	mockDecoder1.EXPECT().NextToken().Return(&models.Token{Type: models.TokenTypeRecord}, nil).Once()
	mockDecoder1.EXPECT().NextToken().Return((*models.Token)(nil), io.EOF).Once()

	mockDecoder2 := mocks.NewMockDecoder[*models.Token](t)
	mockDecoder2.EXPECT().NextToken().Return(&models.Token{Type: models.TokenTypeUDF}, nil).Once()

	currentDecoder := mockDecoder1
	convertFn := func(io.ReadCloser, uint64, string) Decoder[*models.Token] {
		defer func() {
			currentDecoder = mockDecoder2
		}()
		return currentDecoder
	}

	tr := newTokenReader(readersCh, logger, convertFn)

	// Read from first decoder
	token, err := tr.Read()
	assert.NoError(t, err)
	assert.NotNil(t, token)
	assert.Equal(t, models.TokenTypeRecord, token.Type)

	mockReader1.On("Close").Return(nil).Once()

	// Read from second decoder
	token, err = tr.Read()
	assert.NoError(t, err)
	assert.NotNil(t, token)
	assert.Equal(t, models.TokenTypeUDF, token.Type)

	mockDecoder1.AssertExpectations(t)
	mockDecoder2.AssertExpectations(t)
	mockReader1.AssertExpectations(t)
}

func TestTokenReader_ReadFromClosedChannel(t *testing.T) {
	logger := slog.Default()
	readersCh := make(chan models.File)
	close(readersCh)

	tr := newTokenReader[*models.Token](readersCh, logger, nil)

	token, err := tr.Read()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, token)
}

func TestTokenReader_ReadWithDecoderError(t *testing.T) {
	logger := slog.Default()
	readersCh := make(chan models.File, 1)
	mockReader := new(MockReadCloser)
	readersCh <- models.File{Reader: mockReader}

	mockDecoder := mocks.NewMockDecoder[*models.Token](t)
	expectedErr := io.ErrUnexpectedEOF
	mockDecoder.EXPECT().NextToken().Return((*models.Token)(nil), expectedErr).Once()

	convertFn := func(io.ReadCloser, uint64, string) Decoder[*models.Token] {
		return mockDecoder
	}

	tr := newTokenReader(readersCh, logger, convertFn)

	token, err := tr.Read()
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Nil(t, token)

	mockDecoder.AssertExpectations(t)
}

func TestTokenReader_Close(t *testing.T) {
	logger := slog.Default()
	readersCh := make(chan models.File)
	tr := newTokenReader[*models.Token](readersCh, logger, nil)

	// Close is a no-op, so we just ensure it doesn't panic
	assert.NotPanics(t, func() {
		tr.Close()
	})
}
