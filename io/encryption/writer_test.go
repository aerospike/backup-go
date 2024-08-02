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
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncryptedWriterAndReader(t *testing.T) {
	buf := new(bytes.Buffer)
	key := generateKey(t)

	encryptedWriter, err := NewWriter(&writeCloserBuffer{buf}, key)
	assert.NoError(t, err)

	testData := []byte("Hello, encrypted world!")
	n, err := encryptedWriter.Write(testData)
	assert.NoError(t, err)
	assert.Equal(t, len(testData), n)
	err = encryptedWriter.Close()
	assert.NoError(t, err)

	assert.NotEqual(t, testData, buf.Bytes())

	encryptedReader, err := NewEncryptedReader(io.NopCloser(buf), key)
	assert.NoError(t, err)

	decrypted := make([]byte, len(testData))
	n, err = io.ReadFull(encryptedReader, decrypted)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read decrypted data: %v", err)
	}

	assert.Equal(t, decrypted, testData)
	assert.Equal(t, n, len(testData))
}

func TestNegative(t *testing.T) {
	buf := new(bytes.Buffer)
	encryptedWriter, _ := NewWriter(&writeCloserBuffer{buf}, generateKey(t))

	testData := []byte("Hello, encrypted world!")
	_, _ = encryptedWriter.Write(testData)
	_ = encryptedWriter.Close()

	encryptedReader, _ := NewEncryptedReader(io.NopCloser(buf), generateKey(t))

	decrypted := make([]byte, len(testData))
	_, _ = io.ReadFull(encryptedReader, decrypted)

	// data written and read with different keys should not be equal
	assert.NotEqual(t, decrypted, testData)
}

func generateKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	_, err := io.ReadFull(rand.Reader, key)
	assert.NoError(t, err)
	return key
}

type writeCloserBuffer struct {
	*bytes.Buffer
}

func (wcb *writeCloserBuffer) Close() error {
	// No-op close for testing purposes
	return nil
}
