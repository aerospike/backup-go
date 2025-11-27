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
	"encoding/binary"
	"fmt"
	"io"
)

// NewEncryptedReader create new reader, decrypting data from underlying reader
// with a key.
func NewEncryptedReader(r io.ReadCloser, key []byte) (io.ReadCloser, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create new cipher.Block: %w", err)
	}

	iv := make([]byte, aes.BlockSize)
	if _, err = io.ReadFull(r, iv); err != nil {
		return nil, fmt.Errorf("failed to read from buffer: %w", err)
	}
	// Decrypt the IV value
	block.Decrypt(iv, iv)
	// For compatibility with the original asbackup format
	ctr128AddTo(iv, iv, 1)

	stream := cipher.NewCTR(block, iv)

	return &encryptedReader{
		reader: r,
		stream: stream,
	}, nil
}

// Read reads the data from the reader and decrypts it.
func (er *encryptedReader) Read(p []byte) (int, error) {
	n, err := er.reader.Read(p)
	if n > 0 {
		er.stream.XORKeyStream(p[:n], p[:n])
	}

	return n, err
}

// Close closes the reader and returns the error from the underlying reader.
func (er *encryptedReader) Close() error {
	return er.reader.Close()
}

// ctr128AddTo adds the value "val" to the 128-bit integer stored at counter
// in big-endian format. src and dst may overlap.
// This function is copied from asbackup's _ctr128_add_to function which it uses
// to increment its IV.
// This function is only needed to increment the IV after reading it from the
// backup file. After that, the IV is incremented by the CTR mode decryptor.
func ctr128AddTo(dst, src []byte, val uint64) {
	v1 := binary.BigEndian.Uint64(src[:8])
	v2 := binary.BigEndian.Uint64(src[8:])

	v2 += val
	overflow := v2 < val

	if overflow {
		v1++
	}

	binary.BigEndian.PutUint64(dst[:8], v1)
	binary.BigEndian.PutUint64(dst[8:], v2)
}
