package writers

import (
	"io"
	"sync/atomic"
)

type CountingWriter struct {
	writer io.WriteCloser
	count  *atomic.Uint64
}

func NewCountingWriter(w io.WriteCloser, count *atomic.Uint64) *CountingWriter {
	return &CountingWriter{
		writer: w,
		count:  count,
	}
}

func (cw *CountingWriter) Write(p []byte) (n int, err error) {
	n, err = cw.writer.Write(p)
	cw.count.Add(uint64(n))
	return n, err
}

func (cw *CountingWriter) Close() error {
	return cw.writer.Close()
}
