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
	"io"

	"github.com/aerospike/aerospike-tools-backup-lib/encoding/asb"
	"github.com/aerospike/aerospike-tools-backup-lib/models"
)

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

// ASBDecoderBuilder satisfies the DecoderBuilder interface
// It creates a new ASB format decoder
type ASBDecoderBuilder struct {
	src io.Reader
}

// NewASBDecoderBuilder returns a new ASBDecoderBuilder
func NewASBDecoderBuilder() *ASBDecoderBuilder {
	return &ASBDecoderBuilder{}
}

// SetSource sets the source for the ASBDecoder
// This is method is called by the backup client to set the source
// Users of the backup client should not call this method
func (f *ASBDecoderBuilder) SetSource(src io.Reader) {
	f.src = src
}

// CreateDecoder creates a new ASBDecoder
// This method is called by the backup client to create a new decoder
func (f *ASBDecoderBuilder) CreateDecoder() (Decoder, error) {
	return asb.NewDecoder(f.src)
}
