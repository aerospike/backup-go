package backuplib

import (
	"backuplib/decoder"
	"io"
)

// ASBDecoderBuilder satisfies the DecoderBuilder interface
// It creates a new ASB format decoder
type ASBDecoderBuilder struct {
	src io.Reader
}

// NewASBDecoderBuilder returns a new ASBDecoderBuilder
func NewASBDecoderBuilder() *ASBDecoderBuilder {
	return &ASBDecoderBuilder{}
}

// SetSource sets the source for the ASBDecoder
// This is method is called by the backup client to set the source
// Users of the backup client should not call this method
func (f *ASBDecoderBuilder) SetSource(src io.Reader) {
	f.src = src
}

// CreateDecoder creates a new ASBDecoder
// This method is called by the backup client to create a new decoder
func (f *ASBDecoderBuilder) CreateDecoder() (Decoder, error) {
	return decoder.NewASBDecoder(f.src)
}
