package backuplib

import (
	"backuplib/encoder"
	"io"
)

type ASBEncoderBuilder struct {
	dst io.Writer
}

func NewASBEncoderBuilder() *ASBEncoderBuilder {
	return &ASBEncoderBuilder{}
}

func (f *ASBEncoderBuilder) SetDestination(dst io.Writer) {
	f.dst = dst
}

func (f *ASBEncoderBuilder) CreateEncoder() (Encoder, error) {
	return encoder.NewASBEncoder(
		f.dst,
	)
}
