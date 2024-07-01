package writers

import (
	"io"

	"github.com/klauspost/compress/zstd"
)

type compressedWriter struct {
	w          io.WriteCloser
	zstdWriter io.WriteCloser
}

// NewCompressedWriter creates a new instance of compressed_writer
func NewCompressedWriter(w io.WriteCloser, level int) (io.WriteCloser, error) {
	zstWriter, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)))
	if err != nil {
		return nil, err
	}

	return &compressedWriter{
		w:          w,
		zstdWriter: zstWriter,
	}, nil
}

func (cw *compressedWriter) Write(data []byte) (int, error) {
	return cw.zstdWriter.Write(data)
}

func (cw *compressedWriter) Close() error {
	err := cw.zstdWriter.Close()
	if err != nil {
		return err
	}

	return cw.w.Close()
}
