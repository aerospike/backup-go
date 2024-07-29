package backup

import (
	"io"

	"github.com/aerospike/backup-go/encoding/asb"
)

// EncoderType custom type for encoder types enum.
type EncoderType int

const (
	// EncoderTypeASB matches ASB encoder with id 0.
	EncoderTypeASB EncoderType = iota
)

// NewEncoder returns new encoder according to `EncoderType`
func NewEncoder(eType EncoderType, namespace string) encoder {
	switch eType {
	// As at the moment only one `ASB` encoder supported, we use such construction.
	case EncoderTypeASB:
		return asb.NewEncoder(namespace)
	default:
		return asb.NewEncoder(namespace)
	}
}

// NewDecoder returns new decoder according to `EncoderType`
func NewDecoder(eType EncoderType, src io.Reader) (decoder, error) {
	switch eType {
	// As at the moment only one `ASB` decoder supported, we use such construction.
	case EncoderTypeASB:
		return asb.NewDecoder(src)
	default:
		return asb.NewDecoder(src)
	}
}
