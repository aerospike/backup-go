// Copyright 2024-2024 Aerospike, Inc.
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

package encoding

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

// EncoderFactory is used to specify the encoder with which to encode the backup data
type EncoderFactory interface {
	CreateEncoder(namespace string) Encoder
}
