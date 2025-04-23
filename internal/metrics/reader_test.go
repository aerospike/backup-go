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

package metrics

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

// mockReadCloser is a mock implementation of io.ReadCloser for testing
type mockReadCloser struct {
	readFunc  func(p []byte) (n int, err error)
	closeFunc func() error
}

func (m *mockReadCloser) Read(p []byte) (n int, err error) {
	return m.readFunc(p)
}

func (m *mockReadCloser) Close() error {
	return m.closeFunc()
}

func TestNewReader(t *testing.T) {
	t.Parallel()

	mockReader := &mockReadCloser{
		readFunc:  func(_ []byte) (n int, err error) { return 0, nil },
		closeFunc: func() error { return nil },
	}

	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	collector := NewCollector(ctx, logger, MetricKilobytesPerSecond, true)

	reader := NewReader(mockReader, collector)

	assert.NotNil(t, reader)
	assert.Equal(t, mockReader, reader.reader)
	assert.Equal(t, collector, reader.collector)
}

func TestReader_Read(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		readBytes        int
		readErr          error
		collectorEnabled bool
		expectedBytes    int
		expectedErr      error
	}{
		{
			name:             "successful read with enabled collector",
			readBytes:        10,
			readErr:          nil,
			collectorEnabled: true,
			expectedBytes:    10,
			expectedErr:      nil,
		},
		{
			name:             "successful read with disabled collector",
			readBytes:        10,
			readErr:          nil,
			collectorEnabled: false,
			expectedBytes:    10,
			expectedErr:      nil,
		},
		{
			name:             "read with error",
			readBytes:        5,
			readErr:          errors.New("read error"),
			collectorEnabled: true,
			expectedBytes:    5,
			expectedErr:      errors.New("read error"),
		},
		{
			name:             "read with EOF",
			readBytes:        0,
			readErr:          io.EOF,
			collectorEnabled: true,
			expectedBytes:    0,
			expectedErr:      io.EOF,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockReader := &mockReadCloser{
				readFunc: func(p []byte) (n int, err error) {
					// Fill the buffer with some data if readBytes > 0
					if tc.readBytes > 0 {
						for i := 0; i < tc.readBytes && i < len(p); i++ {
							p[i] = 'a'
						}
					}
					return tc.readBytes, tc.readErr
				},
				closeFunc: func() error { return nil },
			}

			ctx := context.Background()
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			collector := NewCollector(ctx, logger, MetricKilobytesPerSecond, tc.collectorEnabled)

			reader := NewReader(mockReader, collector)

			buf := make([]byte, 100)
			n, err := reader.Read(buf)

			assert.Equal(t, tc.expectedBytes, n)
			if tc.expectedErr != nil {
				assert.Error(t, err)
				assert.Equal(t, tc.expectedErr.Error(), err.Error())
			} else {
				assert.NoError(t, err)
			}

			if tc.collectorEnabled && tc.readBytes > 0 {
				assert.Equal(t, uint64(tc.readBytes), collector.counter.Load())
			}
		})
	}
}

func TestReader_Close(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		closeErr    error
		expectedErr error
	}{
		{
			name:        "successful close",
			closeErr:    nil,
			expectedErr: nil,
		},
		{
			name:        "close with error",
			closeErr:    errors.New("close error"),
			expectedErr: errors.New("close error"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockReader := &mockReadCloser{
				readFunc:  func(_ []byte) (n int, err error) { return 0, nil },
				closeFunc: func() error { return tc.closeErr },
			}

			ctx := context.Background()
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			collector := NewCollector(ctx, logger, MetricKilobytesPerSecond, true)

			reader := NewReader(mockReader, collector)

			err := reader.Close()

			if tc.expectedErr != nil {
				assert.Error(t, err)
				assert.Equal(t, tc.expectedErr.Error(), err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
