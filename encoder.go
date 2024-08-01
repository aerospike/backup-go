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

// newEncoder returns new Encoder according to `EncoderType`
func newEncoder(eType EncoderType, namespace string) Encoder {
	switch eType {
	// As at the moment only one `ASB` Encoder supported, we use such construction.
	case EncoderTypeASB:
		return asb.NewEncoder(namespace)
	default:
		return asb.NewEncoder(namespace)
	}
}

// newDecoder returns new decoder according to `EncoderType`
func newDecoder(eType EncoderType, src io.Reader) (decoder, error) {
	switch eType {
	// As at the moment only one `ASB` decoder supported, we use such construction.
	case EncoderTypeASB:
		return asb.NewDecoder(src)
	default:
		return asb.NewDecoder(src)
	}
}
