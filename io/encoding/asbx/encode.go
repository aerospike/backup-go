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
	// version of asbx file.
	version = 1
	// maxPayloadSize according to TCP protocol.
	maxPayloadSize = 128 * 1024 * 1024
	headerSize     = 44
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
	return fmt.Sprintf("%s_%d.asbx", e.namespace, e.fileNumber.Add(1))
}

// EncodeToken encodes a token to the ASBX format.
// It returns a byte slice of the encoded token and an error if the encoding
// fails.
func (e *Encoder) EncodeToken(token *models.ASBXToken) ([]byte, error) {
	// Message contains:
	// Digest - 20 bytes.
	// Payload Size - 6 bytes.
	// Payload - contains a raw message from tcp protocol.
	pLen := len(token.Payload)

	msg := make([]byte, 26+pLen)

	copy(msg[:20], token.Key.Digest())

	// Fill payload len.
	msg[20] = byte(pLen >> 40)
	msg[21] = byte(pLen >> 32)
	msg[22] = byte(pLen >> 24)
	msg[23] = byte(pLen >> 16)
	msg[24] = byte(pLen >> 8)
	msg[25] = byte(pLen)

	// Fill token.
	copy(msg[26:], token.Payload)

	return msg, nil
}

// GetHeader returns prepared file header as []byte.
func (e *Encoder) GetHeader() []byte {
	// Header has fixed size of 40 bytes and contains:
	// Version - 1 byte.
	// File number - 8 bytes.
	// Reserved - 4 bytes.
	// Namespace - 31 byte.
	head := make([]byte, headerSize)

	// Set version. 1 byte.
	head[0] = byte(version)

	// File number (8 bytes)
	num := e.fileNumber.Load()
	head[1] = byte(num >> 56)
	head[2] = byte(num >> 48)
	head[3] = byte(num >> 40)
	head[4] = byte(num >> 32)
	head[5] = byte(num >> 24)
	head[6] = byte(num >> 16)
	head[7] = byte(num >> 8)
	head[8] = byte(num)

	// Reserved (4 bytes) are already zeroed.

	// Namespace (31 bytes).
	copy(head[13:], e.namespaceBytes)

	return head
}

// stringToField writes any string to fixed size []byte slice.
func stringToField(s string, size int) []byte {
	field := make([]byte, size)

	// If the string is shorter than size, it will be right-padded with zeros.
	// If string is longer than size, it will be truncated.
	strBytes := []byte(s)
	copy(field, strBytes)

	return field
}
