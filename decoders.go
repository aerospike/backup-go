package backuplib

import (
	"backuplib/decoder"
	"backuplib/handlers"
	"io"
)

type ASBDecoderBuilder struct {
	src io.Reader
}

func NewASBDecoderBuilder() *ASBDecoderBuilder {
	return &ASBDecoderBuilder{}
}

func (f *ASBDecoderBuilder) SetSource(src io.Reader) {
	f.src = src
}

func (f *ASBDecoderBuilder) CreateDecoder() (handlers.Decoder, error) {
	return decoder.NewASBDecoder(f.src)
}
