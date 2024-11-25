package lazy

import (
	"context"
	"fmt"
	"io"
)

// Writer wraps an io.WriteCloser and creates a writer only on Write operation.
type Writer struct {
	ctx    context.Context // stored internally to be used by the Write method
	writer io.WriteCloser
	open   func(context.Context) (io.WriteCloser, error)
}

// NewWriter creates a new lazy Writer.
func NewWriter(ctx context.Context,
	open func(context.Context) (io.WriteCloser, error),
) (*Writer, error) {
	return &Writer{
		ctx:  ctx,
		open: open,
	}, nil
}

func (f *Writer) Write(p []byte) (n int, err error) {
	if f.writer == nil {
		f.writer, err = f.open(f.ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to open writer: %w", err)
		}
	}

	n, err = f.writer.Write(p)

	return n, err
}

func (f *Writer) Close() error {
	if f.writer == nil { // in case there were no writes
		return nil
	}

	return f.writer.Close()
}
