package counter

import (
	"io"
	"sync/atomic"
)

// Writer counts total number of bytes written with it.
// We need it to get actual number of bytes written, after compression and encryption.
type Writer struct {
	writer io.WriteCloser
	count  *atomic.Uint64
}

func NewWriter(w io.WriteCloser, count *atomic.Uint64) *Writer {
	return &Writer{
		writer: w,
		count:  count,
	}
}

func (cw *Writer) Write(p []byte) (n int, err error) {
	n, err = cw.writer.Write(p)
	cw.count.Add(uint64(n))

	return n, err
}

func (cw *Writer) Close() error {
	return cw.writer.Close()
}
