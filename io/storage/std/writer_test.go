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

package std

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewWriter(t *testing.T) {
	tests := []struct {
		name    string
		ctx     context.Context
		buffer  int
		wantErr bool
	}{
		{
			name:    "success",
			ctx:     context.Background(),
			buffer:  0,
			wantErr: false,
		},
		{
			name:    "cancelled context",
			ctx:     func() context.Context { ctx, cancel := context.WithCancel(context.Background()); cancel(); return ctx }(),
			buffer:  0,
			wantErr: true,
		},
		{
			name:    "negative buffer",
			ctx:     context.Background(),
			buffer:  -10,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer, err := NewWriter(tt.ctx, tt.buffer)

			switch tt.wantErr {
			case true:
				if err == nil {
					t.Error("NewWriter() expected error but got none")
				}
				if writer != nil {
					t.Error("NewWriter() expected nil writer on error")
				}
			case false:
				if err != nil {
					t.Errorf("NewWriter() unexpected error = %v", err)
				}
				if writer == nil {
					t.Error("NewWriter() returned nil writer")
				}
			}
		})
	}
}

func TestWriter_NewWriter(t *testing.T) {
	tests := []struct {
		name     string
		ctx      context.Context
		filename string
		wantErr  bool
	}{
		{
			name:     "success with valid filename",
			ctx:      context.Background(),
			filename: "test.dat",
			wantErr:  false,
		},
		{
			name:     "success with empty filename",
			ctx:      context.Background(),
			filename: "",
			wantErr:  false,
		},
		{
			name:     "success filename ignored anyway",
			ctx:      context.Background(),
			filename: "/invalid/path/file.dat",
			wantErr:  false,
		},
		{
			name:     "error with cancelled context",
			ctx:      func() context.Context { ctx, cancel := context.WithCancel(context.Background()); cancel(); return ctx }(),
			filename: "test.dat",
			wantErr:  true,
		},
		{
			name: "error with deadline exceeded",
			ctx: func() context.Context {
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
				defer cancel()
				return ctx
			}(),
			filename: "test.dat",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Writer{}

			oldStdout := os.Stdout
			r, writer, _ := os.Pipe()
			os.Stdout = writer

			writeCloser, err := w.NewWriter(tt.ctx, tt.filename)

			writer.Close()
			os.Stdout = oldStdout
			r.Close()

			switch tt.wantErr {
			case true:
				if err == nil {
					t.Error("NewWriter() expected error but got none")
				}
				if writeCloser != nil {
					t.Error("NewWriter() expected nil writeCloser on error")
				}
			case false:
				if err != nil {
					t.Errorf("NewWriter() unexpected error = %v", err)
				}
				if writeCloser == nil {
					t.Error("NewWriter() returned nil writeCloser")
				}

				if _, ok := writeCloser.(*stdoutWriteCloser); !ok {
					t.Errorf("NewWriter() returned wrong type, got %T", writeCloser)
				}
			}
		})
	}
}

func TestStdoutWriteCloser_Write(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantLen int
		wantErr bool
	}{
		{
			name:    "write simple data",
			data:    []byte("test data"),
			wantLen: 9,
			wantErr: false,
		},
		{
			name:    "write empty data",
			data:    []byte{},
			wantLen: 0,
			wantErr: false,
		},
		{
			name:    "write large data",
			data:    bytes.Repeat([]byte("x"), 10000),
			wantLen: 10000,
			wantErr: false,
		},
		{
			name:    "write binary data",
			data:    []byte{0x00, 0x01, 0x02, 0xFF, 0xFE},
			wantLen: 5,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			writer := &Writer{}
			writeCloser, err := writer.NewWriter(context.Background(), "test")
			if err != nil {
				t.Fatalf("Failed to create writer: %v", err)
			}

			n, err := writeCloser.Write(tt.data)
			require.NoError(t, err)

			writeCloser.Close()
			w.Close()
			os.Stdout = oldStdout

			var buf bytes.Buffer
			_, err = io.Copy(&buf, r)
			require.NoError(t, err)
			r.Close()

			switch tt.wantErr {
			case true:
				if err == nil {
					t.Error("Write() expected error but got none")
				}
			case false:
				if err != nil {
					t.Errorf("Write() unexpected error = %v", err)
				}
				if n != tt.wantLen {
					t.Errorf("Write() returned %d bytes, expected %d", n, tt.wantLen)
				}
				if !bytes.Equal(buf.Bytes(), tt.data) {
					t.Errorf("Write() output mismatch, got %v, want %v", buf.Bytes(), tt.data)
				}
			}
		})
	}
}

func TestStdoutWriteCloser_Close(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "close after write",
			data:    []byte("test data before close"),
			wantErr: false,
		},
		{
			name:    "close without write",
			data:    nil,
			wantErr: false,
		},
		{
			name:    "close with large buffer",
			data:    bytes.Repeat([]byte("large data "), 1000),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			writer := &Writer{}
			writeCloser, _ := writer.NewWriter(context.Background(), "test")

			if tt.data != nil {
				_, err := writeCloser.Write(tt.data)
				require.NoError(t, err)
			}

			err := writeCloser.Close()
			require.NoError(t, err)
			w.Close()
			os.Stdout = oldStdout

			var buf bytes.Buffer
			_, err = io.Copy(&buf, r)
			require.NoError(t, err)
			r.Close()

			switch tt.wantErr {
			case true:
				if err == nil {
					t.Error("Close() expected error but got none")
				}
			case false:
				if err != nil {
					t.Errorf("Close() unexpected error = %v", err)
				}
				if tt.data != nil && !bytes.Equal(buf.Bytes(), tt.data) {
					t.Errorf("Close() didn't flush data properly")
				}
			}
		})
	}
}

func TestConcurrentWrites(t *testing.T) {
	t.Parallel()

	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	writer := &Writer{}

	const numGoroutines = 10
	const writesPerGoroutine = 100

	var wg sync.WaitGroup
	writers := make([]io.WriteCloser, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		writeCloser, err := writer.NewWriter(context.Background(), "concurrent-test")
		if err != nil {
			t.Fatalf("Failed to create writer %d: %v", i, err)
		}
		writers[i] = writeCloser
	}

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int, writeCloser io.WriteCloser) {
			defer wg.Done()
			defer writeCloser.Close()

			for j := 0; j < writesPerGoroutine; j++ {
				data := []byte("goroutine-" + string(rune(id+'0')) + "-write-" + string(rune(j+'0')) + "\n")
				_, err := writeCloser.Write(data)
				if err != nil {
					t.Errorf("Concurrent write failed: %v", err)
				}
			}
		}(i, writers[i])
	}

	wg.Wait()
	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, err := io.Copy(&buf, r)
	require.NoError(t, err)
	r.Close()

	if buf.Len() == 0 {
		t.Error("No data written in concurrent test")
	}
}

// Test to increase coverage.
func TestStdout_Remove(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	w, err := NewWriter(ctx, defaultBufferSize)
	require.NoError(t, err)

	err = w.RemoveFiles(ctx)
	require.NoError(t, err)

	err = w.Remove(ctx, "")
	require.NoError(t, err)
}

func TestStdout_GetType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	w, err := NewWriter(ctx, defaultBufferSize)
	require.NoError(t, err)

	res := w.GetType()
	require.Equal(t, stdoutType, res)
}
