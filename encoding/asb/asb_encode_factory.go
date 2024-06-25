package asb

import (
	"github.com/aerospike/backup-go/encoding"
)

// asbEncoderFactory satisfies the encoding.EncoderFactory interface
// It creates a new ASB format encoder
type asbEncoderFactory struct {
}

// NewASBEncoderFactory returns a new asbEncoderFactory
func NewASBEncoderFactory() encoding.EncoderFactory {
	return &asbEncoderFactory{}
}

var _ encoding.EncoderFactory = (*asbEncoderFactory)(nil)

// CreateEncoder creates a new ASBEncoder
// This method is called by the backup client to create a new encoder
func (f *asbEncoderFactory) CreateEncoder(namespace string) encoding.Encoder {
	return NewEncoder(namespace)
}
