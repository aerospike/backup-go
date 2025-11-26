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

package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
)

type writer struct {
	writer io.WriteCloser
	stream cipher.Stream
}

// NewWriter returns a new encryption Writer
// Writes to the returned writer are encrypted with a key and written to w.
func NewWriter(w io.WriteCloser, key []byte) (io.WriteCloser, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create new cipher.Block: %w", err)
	}

	// Create a random Initialization Vector
	iv := make([]byte, aes.BlockSize)
	if _, err = io.ReadFull(rand.Reader, iv); err != nil {
		return nil, fmt.Errorf("failed to read from buffer: %w", err)
	}

	// Encrypt the IV before writing
	encIv := make([]byte, aes.BlockSize)
	// For compatibility with the original asbackup format
	ctr128SubtractFrom(encIv, iv, 1)
	block.Encrypt(encIv, encIv)

	// Write the IV to the underlying writer
	if _, err = w.Write(encIv); err != nil {
		return nil, fmt.Errorf("failed to write: %w", err)
	}

	stream := cipher.NewCTR(block, iv)

	return &writer{
		writer: w,
		stream: stream,
	}, nil
}

// Write writes the data to the writer and encrypts it.
func (ew *writer) Write(p []byte) (int, error) {
	encrypted := make([]byte, len(p))
	ew.stream.XORKeyStream(encrypted, p)

	return ew.writer.Write(encrypted)
}

// Close closes the writer and returns the error from the underlying writer.
func (ew *writer) Close() error {
	return ew.writer.Close()
}

// encryptedReader is a reader that decrypts the data from the underlying reader.
type encryptedReader struct {
	reader io.ReadCloser
	stream cipher.Stream
}

// ctr128SubtractFrom is used to decrement the IV value before writing.
// This is required to be backward compatible with the original asbackup
// format.
func ctr128SubtractFrom(dst, src []byte, val uint64) {
	v1 := binary.BigEndian.Uint64(src[:8])
	v2 := binary.BigEndian.Uint64(src[8:])

	if v2 == 0 {
		v1--
	}

	v2 -= val

	binary.BigEndian.PutUint64(dst[:8], v1)
	binary.BigEndian.PutUint64(dst[8:], v2)
}
