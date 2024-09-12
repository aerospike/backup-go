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
	readersCh := make(chan io.ReadCloser, 1)
	mockReader := new(MockReadCloser)
	readersCh <- mockReader

	mockDecoder := mocks.NewMockDecoder(t)
	mockDecoder.EXPECT().NextToken().Return(&models.Token{Type: models.TokenTypeRecord}, nil).Once()

	convertFn := func(io.ReadCloser) Decoder {
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
	readersCh := make(chan io.ReadCloser, 1)
	mockReader := new(MockReadCloser)
	readersCh <- mockReader

	mockDecoder := mocks.NewMockDecoder(t)
	mockDecoder.EXPECT().NextToken().Return(&models.Token{Type: models.TokenTypeRecord}, nil).Times(3)
	mockDecoder.EXPECT().NextToken().Return((*models.Token)(nil), io.EOF).Once()

	convertFn := func(io.ReadCloser) Decoder {
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
	readersCh := make(chan io.ReadCloser, 2)
	mockReader1 := new(MockReadCloser)
	mockReader2 := new(MockReadCloser)
	readersCh <- mockReader1
	readersCh <- mockReader2

	mockDecoder1 := mocks.NewMockDecoder(t)
	mockDecoder1.EXPECT().NextToken().Return(&models.Token{Type: models.TokenTypeRecord}, nil).Once()
	mockDecoder1.EXPECT().NextToken().Return((*models.Token)(nil), io.EOF).Once()

	mockDecoder2 := mocks.NewMockDecoder(t)
	mockDecoder2.EXPECT().NextToken().Return(&models.Token{Type: models.TokenTypeUDF}, nil).Once()

	currentDecoder := mockDecoder1
	convertFn := func(io.ReadCloser) Decoder {
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
	readersCh := make(chan io.ReadCloser)
	close(readersCh)

	tr := newTokenReader(readersCh, logger, nil)

	token, err := tr.Read()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, token)
}

func TestTokenReader_ReadWithDecoderError(t *testing.T) {
	logger := slog.Default()
	readersCh := make(chan io.ReadCloser, 1)
	mockReader := new(MockReadCloser)
	readersCh <- mockReader

	mockDecoder := mocks.NewMockDecoder(t)
	expectedErr := io.ErrUnexpectedEOF
	mockDecoder.EXPECT().NextToken().Return((*models.Token)(nil), expectedErr).Once()

	convertFn := func(io.ReadCloser) Decoder {
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
	readersCh := make(chan io.ReadCloser)
	tr := newTokenReader(readersCh, logger, nil)

	// Close is a no-op, so we just ensure it doesn't panic
	assert.NotPanics(t, func() {
		tr.Close()
	})
}
