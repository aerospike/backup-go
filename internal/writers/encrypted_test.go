package writers

import (
	"bytes"
	"crypto/rand"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func TestEncryptedWriterAndReader(t *testing.T) {
	buf := new(bytes.Buffer)

	key := make([]byte, 16)
	_, err := io.ReadFull(rand.Reader, key)
	assert.NoError(t, err)

	encryptedWriter, err := NewEncryptedWriter(&writeCloserBuffer{buf}, key)
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
}

type writeCloserBuffer struct {
	*bytes.Buffer
}

func (wcb *writeCloserBuffer) Close() error {
	// No-op close for testing purposes
	return nil
}
