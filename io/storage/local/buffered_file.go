package local

import (
	"bufio"
	"errors"
	"io"
)

// bufferedFile is a wrapper around a `bufio.Writer` and a `io.Closer`.
type bufferedFile struct {
	*bufio.Writer
	closer io.Closer
}

// Close flushes the writer and closes the closer.
func (bf *bufferedFile) Close() error {
	flushErr := bf.Flush()
	closeErr := bf.closer.Close()

	return errors.Join(flushErr, closeErr)
}
