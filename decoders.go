package backuplib

import (
	"backuplib/decoder"
	"backuplib/handlers"
	"io"
)

type ASBReaderBuilder struct {
	src io.Reader
}

func NewASBReaderBuilder() *ASBReaderBuilder {
	return &ASBReaderBuilder{}
}

func (f *ASBReaderBuilder) SetSource(src io.Reader) {
	f.src = src
}

func (f *ASBReaderBuilder) CreateDecoder() (handlers.Decoder, error) {
	return decoder.NewASBReader(f.src)
}
