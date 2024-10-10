// Copyright 2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"io"

	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/models"
)

// EncoderType custom type for Encoder types enum.
type EncoderType int

const (
	// EncoderTypeASB matches ASB Encoder with id 0.
	EncoderTypeASB EncoderType = iota
)

// Encoder is an interface for encoding the types from the models package.
// It is used to support different data formats.
//
//go:generate mockery --name Encoder
type Encoder interface {
	EncodeToken(*models.Token) ([]byte, error)
	GetHeader() []byte
	GenerateFilename(prefix string) string
}

// NewEncoder returns a new Encoder according to `EncoderType`.
func NewEncoder(eType EncoderType, namespace string, compact bool) Encoder {
	switch eType {
	// As at the moment only one `ASB` Encoder supported, we use such construction.
	case EncoderTypeASB:
		return asb.NewEncoder(namespace, compact)
	default:
		return asb.NewEncoder(namespace, compact)
	}
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

// NewDecoder returns a new Decoder according to `EncoderType`.
func NewDecoder(eType EncoderType, src io.Reader) (Decoder, error) {
	switch eType {
	// As at the moment only one `ASB` Decoder supported, we use such construction.
	case EncoderTypeASB:
		return asb.NewDecoder(src)
	default:
		return asb.NewDecoder(src)
	}
}
