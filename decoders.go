package backuplib

import (
	"backuplib/decoder"
	"backuplib/handlers"
	"io"
)

type ASBReaderFactory struct {
	src io.Reader
}

func NewASBReaderFactory(src io.Reader) *ASBReaderFactory {
	return &ASBReaderFactory{
		src: src,
	}
}

func (f *ASBReaderFactory) CreateDecoder() (handlers.Decoder, error) {
	return decoder.NewASBReader(f.src)
}
