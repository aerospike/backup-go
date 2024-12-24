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

package asbx

import (
	"fmt"
	"sync/atomic"

	"github.com/aerospike/backup-go/models"
)

const (
	// asbx file version.
	version = 1
)

// Encoder contains logic for encoding backup data into the binary .asbx format.
// This is a stateful object that must be created for every backup operation.
type Encoder struct {
	namespace      string
	namespaceBytes []byte
	fileNumber     atomic.Int64
}

// NewEncoder creates a new Encoder.
func NewEncoder(namespace string) *Encoder {
	return &Encoder{
		namespace:      namespace,
		namespaceBytes: stringToField(namespace, 31),
	}
}

// GenerateFilename generates a file name for the given namespace.
func (e *Encoder) GenerateFilename() string {
	// TODO: using prefix and suffix for asbx is prohibited.
	// TODO: think about file naming, so we can sort them
	return fmt.Sprintf("%s_%d.asbx", e.namespace, e.fileNumber.Add(1))
}

// EncodeToken encodes a token to the ASBX format.
// It returns a byte slice of the encoded token and an error if the encoding
// fails.
func (e *Encoder) EncodeToken(token *models.XDRToken) ([]byte, error) {
	// Message contains:
	// Digest - 20 bytes
	// Payload Size - 4 bytes
	// Payload - contains a raw message from tcp protocol.
	payloadLen := len(token.Payload)

	message := make([]byte, 24+payloadLen)

	copy(message[:20], token.Key.Digest())

	// Fill payload len.
	message[20] = byte(payloadLen >> 24)
	message[21] = byte(payloadLen >> 16)
	message[22] = byte(payloadLen >> 8)
	message[23] = byte(payloadLen)

	// Fill token.
	copy(message[24:], token.Payload)

	return message, nil
}

// GetHeader returns prepared file header as []byte.
func (e *Encoder) GetHeader() []byte {
	// Header has fixed size of 40 bytes and contains:
	// Version - 1 byte
	// File number - 4 bytes
	// Reserved - 4 bytes
	// Namespace - 31 byte
	head := make([]byte, 40)

	// Set version. 1 byte.
	head[0] = byte(version)

	// File number (4 bytes)
	num := e.fileNumber.Load()
	head[1] = byte(num >> 24)
	head[2] = byte(num >> 16)
	head[3] = byte(num >> 8)
	head[4] = byte(num)

	// Reserved (4 bytes) are already zeroed

	// Namespace (31 bytes)
	copy(head[9:], e.namespaceBytes)

	return head
}

// stringToField writes any string to fixed len []byte slice.
func stringToField(s string, size int) []byte {
	field := make([]byte, size)

	// If string is shorter than size, it will be left-padded with zeros
	// If string is longer than size, it will be truncated
	strBytes := []byte(s)
	copy(field[max(0, size-len(strBytes)):], strBytes[max(0, len(strBytes)-size):])

	return field
}
