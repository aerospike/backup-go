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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

// mockWriteCloser is a mock implementation of io.WriteCloser for testing
type mockWriteCloser struct {
	writeFunc func(p []byte) (n int, err error)
	closeFunc func() error
}

func (m *mockWriteCloser) Write(p []byte) (n int, err error) {
	return m.writeFunc(p)
}

func (m *mockWriteCloser) Close() error {
	return m.closeFunc()
}

func TestNewWriter(t *testing.T) {
	t.Parallel()

	// Create a mock writer
	mockWriter := &mockWriteCloser{
		writeFunc: func(_ []byte) (n int, err error) { return 0, nil },
		closeFunc: func() error { return nil },
	}

	// Create a collector
	ctx := context.Background()
	logger := slog.New(slog.DiscardHandler)
	collector := NewCollector(ctx, logger, KilobytesPerSecond, testMetricMessage, true)

	// Create a new Writer
	writer := NewWriter(mockWriter, collector)

	// Verify the writer was initialized correctly
	assert.NotNil(t, writer)
	assert.Equal(t, mockWriter, writer.writer)
	assert.Equal(t, collector, writer.collector)
}

func TestWriter_Write(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		writeBytes       int
		writeErr         error
		collectorEnabled bool
		expectedBytes    int
		expectedErr      error
	}{
		{
			name:             "successful write with enabled collector",
			writeBytes:       10,
			writeErr:         nil,
			collectorEnabled: true,
			expectedBytes:    10,
			expectedErr:      nil,
		},
		{
			name:             "successful write with disabled collector",
			writeBytes:       10,
			writeErr:         nil,
			collectorEnabled: false,
			expectedBytes:    10,
			expectedErr:      nil,
		},
		{
			name:             "write with error",
			writeBytes:       5,
			writeErr:         errors.New("write error"),
			collectorEnabled: true,
			expectedBytes:    5,
			expectedErr:      errors.New("write error"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Create a mock writer that returns the specified bytes and error
			mockWriter := &mockWriteCloser{
				writeFunc: func(_ []byte) (n int, err error) {
					return tc.writeBytes, tc.writeErr
				},
				closeFunc: func() error { return nil },
			}

			// Create a collector
			ctx := context.Background()
			logger := slog.New(slog.DiscardHandler)
			collector := NewCollector(ctx, logger, KilobytesPerSecond, testMetricMessage, tc.collectorEnabled)

			// Create a new Writer
			writer := NewWriter(mockWriter, collector)

			// Write to the writer
			n, err := writer.Write(make([]byte, 100))

			// Verify the results
			assert.Equal(t, tc.expectedBytes, n)
			if tc.expectedErr != nil {
				assert.Error(t, err)
				assert.Equal(t, tc.expectedErr.Error(), err.Error())
			} else {
				assert.NoError(t, err)
			}

			// Verify the collector was updated correctly
			if tc.collectorEnabled && tc.writeBytes > 0 {
				assert.Equal(t, uint64(tc.writeBytes), collector.processed.Load())
			}
		})
	}
}

func TestWriter_Close(t *testing.T) {
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

			// Create a mock writer that returns the specified close error
			mockWriter := &mockWriteCloser{
				writeFunc: func(_ []byte) (n int, err error) { return 0, nil },
				closeFunc: func() error { return tc.closeErr },
			}

			// Create a collector
			ctx := context.Background()
			logger := slog.New(slog.DiscardHandler)
			collector := NewCollector(ctx, logger, KilobytesPerSecond, testMetricMessage, true)

			// Create a new Writer
			writer := NewWriter(mockWriter, collector)

			// Close the writer
			err := writer.Close()

			// Verify the results
			if tc.expectedErr != nil {
				assert.Error(t, err)
				assert.Equal(t, tc.expectedErr.Error(), err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
