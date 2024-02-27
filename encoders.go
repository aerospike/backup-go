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

package backuplib

import (
	"github.com/aerospike/aerospike-tools-backup-lib/encoding/asb"
	"github.com/aerospike/aerospike-tools-backup-lib/models"
)

// Encoder is an interface for encoding the types from the models package.
// It is used to support different data formats.
//
//go:generate mockery --name Encoder
type Encoder interface {
	EncodeToken(*models.Token) ([]byte, error)
}

// ASBEncoderFactory satisfies the EncoderBuilder interface
// It creates a new ASB format encoder
type ASBEncoderFactory struct{}

// NewASBEncoderFactory returns a new ASBEncoderBuilder
func NewASBEncoderFactory() *ASBEncoderFactory {
	return &ASBEncoderFactory{}
}

// CreateEncoder creates a new ASBEncoder
// This method is called by the backup client to create a new encoder
func (f *ASBEncoderFactory) CreateEncoder() (Encoder, error) {
	return asb.NewEncoder()
}
