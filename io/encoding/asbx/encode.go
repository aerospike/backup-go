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
type Encoder[T models.TokenConstraint] struct {
	namespace      string
	namespaceBytes []byte
	fileNumber     atomic.Int64
}

// NewEncoder creates a new Encoder.
func NewEncoder[T models.TokenConstraint](namespace string) *Encoder[T] {
	return &Encoder[T]{
		namespace:      namespace,
		namespaceBytes: stringToField(namespace, 31),
	}
}

// GenerateFilename generates a file name for the given namespace.
// Empty values are used to implement Encoder interface.
func (e *Encoder[T]) GenerateFilename(prefix, suffix string) string {
	return fmt.Sprintf("%s%s_%d%s.asbx", prefix, e.namespace, e.fileNumber.Add(1), suffix)
}

// EncodeToken encodes a token to the ASBX format.
// It returns a byte slice of the encoded token and an error if the encoding
// fails.
func (e *Encoder[T]) EncodeToken(token T) ([]byte, error) {
	t, ok := any(token).(*models.ASBXToken)
	if !ok {
		return nil, fmt.Errorf("unsupported token type %T for ASBX encoder", token)
	}
	// Message contains:
	// Digest - 20 bytes.
	// Payload Size - 6 bytes.
	// Payload - contains a raw message from tcp protocol.
	pLen := len(t.Payload)

	msg := make([]byte, 26+pLen)

	copy(msg[:20], t.Key.Digest())

	// Fill payload len.
	msg[20] = byte(pLen >> 40)
	msg[21] = byte(pLen >> 32)
	msg[22] = byte(pLen >> 24)
	msg[23] = byte(pLen >> 16)
	msg[24] = byte(pLen >> 8)
	msg[25] = byte(pLen)

	// Fill token.
	copy(msg[26:], t.Payload)

	return msg, nil
}

// GetHeader returns prepared file header as []byte.
func (e *Encoder[T]) GetHeader(fileNumber uint64) []byte {
	// Header has fixed size of 40 bytes and contains:
	// Version - 1 byte.
	// File number - 8 bytes.
	// Reserved - 4 bytes.
	// Namespace - 31 byte.
	head := make([]byte, headerSize)

	// Set version. 1 byte.
	head[0] = byte(version)

	// File number (8 bytes)
	head[1] = byte(fileNumber >> 56)
	head[2] = byte(fileNumber >> 48)
	head[3] = byte(fileNumber >> 40)
	head[4] = byte(fileNumber >> 32)
	head[5] = byte(fileNumber >> 24)
	head[6] = byte(fileNumber >> 16)
	head[7] = byte(fileNumber >> 8)
	head[8] = byte(fileNumber)

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
