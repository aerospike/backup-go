package asb

import (
	"fmt"
	"io"
	"path/filepath"

	"github.com/aerospike/backup-go/encoding"
)

// asbDecoderFactory satisfies the DecoderFactory interface
// It creates a new ASB format decoder
type asbDecoderFactory struct{}

// NewASBDecoderFactory returns a new ASBDecoderBuilder
func NewASBDecoderFactory() encoding.DecoderFactory {
	return &asbDecoderFactory{}
}

var _ encoding.DecoderFactory = (*asbDecoderFactory)(nil)

// CreateDecoder creates a new ASBDecoder
// This method is called by the backup client to create a new decoder
func (f *asbDecoderFactory) CreateDecoder(src io.Reader) (encoding.Decoder, error) {
	return NewDecoder(src)
}

func (f *asbDecoderFactory) Validate(fileName string) error {
	if filepath.Ext(fileName) != ".asb" {
		return fmt.Errorf("restore file %s is in an invalid format, expected extension: .asb, got: %s",
			fileName, filepath.Ext(fileName))
	}

	return nil
}
