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
	"bytes"
	"io"
	"log/slog"

	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/models"
)

// Encoder is an interface for encoding the types from the models package.
// It is used to support different data formats.
type Encoder interface {
	EncodeToken(*models.Token, *bytes.Buffer) error
	GetHeader(bool) []byte
	GenerateFilename(prefix, suffix string) string
}

// NewEncoder returns a new ASB encoder.
func NewEncoder(namespace string, compact bool, sIndexInfo models.SIndexInfo) Encoder {
	cfg := asb.NewEncoderConfig(namespace, compact, sIndexInfo)
	return asb.NewEncoder(cfg)
}

// Decoder is an interface for reading backup data as tokens.
type Decoder interface {
	NextToken() (*models.Token, error)
}

// NewDecoder returns a new ASB decoder.
func NewDecoder(src io.Reader, fileName string, ignoreUnknownFields bool, logger *slog.Logger,
) (Decoder, error) {
	return asb.NewDecoder(src, fileName, ignoreUnknownFields, logger)
}
