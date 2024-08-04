package compression

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

type writer struct {
	w          io.WriteCloser
	zstdWriter io.WriteCloser
}

// NewWriter creates a new instance of a compression writer with a given compression level.
// Every Write to it is compressed and passed to an inner writer.
// On Close, both writers are closed.
func NewWriter(w io.WriteCloser, level int) (io.WriteCloser, error) {
	zstWriter, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd writer: %w", err)
	}

	return &writer{
		w:          w,
		zstdWriter: zstWriter,
	}, nil
}

func (cw *writer) Write(data []byte) (int, error) {
	return cw.zstdWriter.Write(data)
}

func (cw *writer) Close() error {
	err := cw.zstdWriter.Close()
	if err != nil {
		return fmt.Errorf("failed to close zstd writer: %w", err)
	}

	return cw.w.Close()
}
