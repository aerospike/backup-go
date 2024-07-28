package backup

import (
	"io"

	"github.com/aerospike/backup-go/encoding/asb"
	"github.com/aerospike/backup-go/models"
)

type EncoderType int

const (
	EncoderTypeASB EncoderType = iota
)

// Encoder is an interface for encoding the types from the models package.
// It is used to support different data formats.
//
//go:generate mockery --name Encoder
type encoder interface {
	EncodeToken(*models.Token) ([]byte, error)
	GetHeader() []byte
	GenerateFilename() string
}

func NewEncoder(eType EncoderType, namespace string) encoder {
	switch eType {
	case EncoderTypeASB:
		return asb.NewEncoder(namespace)
	default:
		return asb.NewEncoder(namespace)
	}
}

// Decoder is an interface for reading backup data as tokens.
// It is used to support different data formats.
// While the return type is `any`, the actual types returned should
// only be the types exposed by the models package.
// e.g. *models.Record, *models.UDF and *models.SecondaryIndex
//
//go:generate mockery --name Decoder
type decoder interface {
	NextToken() (*models.Token, error)
}

func NewDecoder(eType EncoderType, src io.Reader) (decoder, error) {
	switch eType {
	case EncoderTypeASB:
		return asb.NewDecoder(src)
	default:
		return asb.NewDecoder(src)
	}
}

func EncoderValidate(eType EncoderType) func(string) error {
	switch eType {
	case EncoderTypeASB:
		return asb.Validate
	default:
		return asb.Validate
	}
}
