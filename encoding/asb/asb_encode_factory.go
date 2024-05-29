package asb

import (
	"fmt"

	"github.com/aerospike/backup-go/encoding"
)

// asbEncoderFactory satisfies the asbEncoderFactory interface
// It creates a new ASB format encoder
type asbEncoderFactory struct{}

// NewASBEncoderFactory returns a new asbEncoderFactory
func NewASBEncoderFactory() encoding.EncoderFactory {
	return &asbEncoderFactory{}
}

var _ encoding.EncoderFactory = (*asbEncoderFactory)(nil)

// CreateEncoder creates a new ASBEncoder
// This method is called by the backup client to create a new encoder
func (f *asbEncoderFactory) CreateEncoder() (encoding.Encoder, error) {
	return NewEncoder()
}

// GenerateFilename generates a filename for a given namespace and ID
func (f *asbEncoderFactory) GenerateFilename(namespace string, id uint32) string {
	return fmt.Sprintf("%s_%d.asb", namespace, id)
}
