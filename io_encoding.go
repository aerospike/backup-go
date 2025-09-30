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
	"github.com/aerospike/backup-go/io/encoding/asbx"
	"github.com/aerospike/backup-go/models"
)

// EncoderType custom type for Encoder types enum.
type EncoderType int

const (
	// EncoderTypeASB matches ASB Encoder with id 0.
	EncoderTypeASB EncoderType = iota
	EncoderTypeASBX
)

// Encoder is an interface for encoding the types from the models package.
// It is used to support different data formats.
//
//go:generate mockery --name Encoder
type Encoder[T models.TokenConstraint] interface {
	EncodeToken(T) ([]byte, error)
	GetHeader(uint64) []byte
	GenerateFilename(prefix, suffix string) string
}

// NewEncoder returns a new Encoder according to `EncoderType`.
func NewEncoder[T models.TokenConstraint](eType EncoderType, namespace string, compact, hasExprSindex bool,
) Encoder[T] {
	switch eType {
	// As at the moment only one `ASB` Encoder supported, we use such construction.
	case EncoderTypeASB:
		cfg := asb.NewEncoderConfig(namespace, compact, hasExprSindex)
		return asb.NewEncoder[T](cfg)
	case EncoderTypeASBX:
		return asbx.NewEncoder[T](namespace)
	default:
		cfg := asb.NewEncoderConfig(namespace, compact, hasExprSindex)
		return asb.NewEncoder[T](cfg)
	}
}

// Decoder is an interface for reading backup data as tokens.
// It is used to support different data formats.
// While the return type is `any`, the actual types returned should
// only be the types exposed by the models package.
// e.g. *models.Record, *models.UDF and *models.SecondaryIndex
//
//go:generate mockery --name Decoder
type Decoder[T models.TokenConstraint] interface {
	NextToken() (T, error)
}

// NewDecoder returns a new Decoder according to `EncoderType`.
func NewDecoder[T models.TokenConstraint](eType EncoderType, src io.Reader, fileNumber uint64, fileName string,
) (Decoder[T], error) {
	switch eType {
	// As at the moment only one `ASB` Decoder supported, we use such construction.
	case EncoderTypeASB:
		return asb.NewDecoder[T](src, fileName)
	case EncoderTypeASBX:
		return asbx.NewDecoder[T](src, fileNumber, fileName)
	default:
		return asb.NewDecoder[T](src, fileName)
	}
}
