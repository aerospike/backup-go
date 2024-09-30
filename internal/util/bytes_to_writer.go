package util

import (
	"bytes"
)

// BytesWriteCloser implements io.WriteCloser for bytes.Buffer
type BytesWriteCloser struct {
	buffer *bytes.Buffer
}

// Write writes data.
func (w *BytesWriteCloser) Write(p []byte) (int, error) {
	return w.buffer.Write(p)
}

// Close implements .Close() method for io.WriterCloser
func (w *BytesWriteCloser) Close() error {
	return nil
}

// Buffer returns buffer with data.
func (w *BytesWriteCloser) Buffer() *bytes.Buffer {
	return w.buffer
}

// NewBytesWriteCloser returns io.WriteCloser for []bytes
func NewBytesWriteCloser(p []byte) *BytesWriteCloser {
	return &BytesWriteCloser{buffer: bytes.NewBuffer(p)}
}
