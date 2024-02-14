package backuplib

import (
	"backuplib/encoder"
	"io"
)

type ASBEncoderFactory struct {
	dst io.Writer
}

func NewASBEncoderFactory() *ASBEncoderFactory {
	return &ASBEncoderFactory{}
}

func (f *ASBEncoderFactory) SetDestination(dst io.Writer) {
	f.dst = dst
}

func (f *ASBEncoderFactory) CreateEncoder() (Encoder, error) {
	return encoder.NewASBEncoder(
		f.dst,
	)
}
