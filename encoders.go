package backuplib

import (
	"backuplib/encoder"
	"io"
)

// ASBEncoderBuilder satisfies the EncoderBuilder interface
// It creates a new ASB format encoder
type ASBEncoderBuilder struct {
	dst io.Writer
}

// NewASBEncoderBuilder returns a new ASBEncoderBuilder
func NewASBEncoderBuilder() *ASBEncoderBuilder {
	return &ASBEncoderBuilder{}
}

// SetDestination sets the destination for the ASBEncoder
// This is method is called by the backup client to set the destination
// Users of the backup client should not call this method
func (f *ASBEncoderBuilder) SetDestination(dst io.Writer) {
	f.dst = dst
}

// CreateEncoder creates a new ASBEncoder
// This method is called by the backup client to create a new encoder
func (f *ASBEncoderBuilder) CreateEncoder() (Encoder, error) {
	return encoder.NewASBEncoder(
		f.dst,
	)
}
