package interfaces

import (
	"github.com/aerospike/backup-go/models"
)

// Encoder is an interface for encoding the types from the models package.
// It is used to support different data formats.
//
//go:generate mockery --name Encoder
type Encoder interface {
	EncodeToken(*models.Token) ([]byte, error)
	GetHeader() []byte
	GenerateFilename() string
}

// Decoder is an interface for reading backup data as tokens.
// It is used to support different data formats.
// While the return type is `any`, the actual types returned should
// only be the types exposed by the models package.
// e.g. *models.Record, *models.UDF and *models.SecondaryIndex
//
//go:generate mockery --name Decoder
type Decoder interface {
	NextToken() (*models.Token, error)
}

// Validator interface that describes backup files validator.
// Must be part of encoder implementation.
//
//go:generate mockery --name Validator
type Validator interface {
	Run(fileName string) error
}
