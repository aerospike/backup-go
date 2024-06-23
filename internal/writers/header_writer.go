package writers

import (
	"io"
	"sync"
)

type headerWriter struct {
	writer    io.WriteCloser
	header    func() []byte
	writeOnce sync.Once
}

func NewHeaderWriter(writer io.WriteCloser, header func() []byte) io.WriteCloser {
	return &headerWriter{
		writer: writer,
		header: header,
	}
}

func (h *headerWriter) Write(p []byte) (n int, err error) {
	headerSize := 0
	h.writeOnce.Do(func() {
		if headerSize, err = h.writer.Write(h.header()); err != nil {
			n = 0
			return
		}
	})

	n, err = h.writer.Write(p)

	return n + headerSize, err
}

func (h *headerWriter) Close() error {
	_, err := h.writer.Write([]byte{}) // need to call write at least once to write header
	if err != nil {
		return err
	}

	return h.writer.Close()
}
