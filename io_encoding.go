package backup

import (
	"io"

	"github.com/aerospike/backup-go/io/encoding/asb"
)

// EncoderType custom type for Encoder types enum.
type EncoderType int

const (
	// EncoderTypeASB matches ASB Encoder with id 0.
	EncoderTypeASB EncoderType = iota
)

// NewEncoder returns new Encoder according to `EncoderType`
func NewEncoder(eType EncoderType, namespace string) Encoder {
	switch eType {
	// As at the moment only one `ASB` Encoder supported, we use such construction.
	case EncoderTypeASB:
		return asb.NewEncoder(namespace)
	default:
		return asb.NewEncoder(namespace)
	}
}

// NewDecoder returns new Decoder according to `EncoderType`
func NewDecoder(eType EncoderType, src io.Reader) (Decoder, error) {
	switch eType {
	// As at the moment only one `ASB` Decoder supported, we use such construction.
	case EncoderTypeASB:
		return asb.NewDecoder(src)
	default:
		return asb.NewDecoder(src)
	}
}
