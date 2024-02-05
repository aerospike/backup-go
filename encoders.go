package backuplib

import (
	"backuplib/encoder"
	"backuplib/handlers"
)

type ASBEncoderFactory struct{}

func NewASBEncoderFactory() *ASBEncoderFactory {
	return &ASBEncoderFactory{}
}

func (f *ASBEncoderFactory) CreateEncoder() handlers.Encoder {
	return encoder.NewASBEncoder()
}
