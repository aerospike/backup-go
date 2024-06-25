package writers

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
)

type EncryptedWriter struct {
	writer io.WriteCloser
	stream cipher.Stream
}

func NewEncryptedWriter(w io.WriteCloser, key []byte) (*EncryptedWriter, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// Create a random Initialization Vector
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}

	// Write the IV to the underlying writer
	if _, err := w.Write(iv); err != nil {
		return nil, err
	}

	stream := cipher.NewCTR(block, iv)

	return &EncryptedWriter{
		writer: w,
		stream: stream,
	}, nil
}

func (ew *EncryptedWriter) Write(p []byte) (int, error) {
	encrypted := make([]byte, len(p))
	ew.stream.XORKeyStream(encrypted, p)
	return ew.writer.Write(encrypted)
}

func (ew *EncryptedWriter) Close() error {
	return ew.writer.Close()
}

// EncryptedReader struct
type EncryptedReader struct {
	reader io.ReadCloser
	stream cipher.Stream
}

// NewEncryptedReader function
func NewEncryptedReader(r io.ReadCloser, key []byte) (*EncryptedReader, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(r, iv); err != nil {
		return nil, err
	}

	stream := cipher.NewCTR(block, iv)

	return &EncryptedReader{
		reader: r,
		stream: stream,
	}, nil
}

func (er *EncryptedReader) Read(p []byte) (int, error) {
	n, err := er.reader.Read(p)
	if n > 0 {
		er.stream.XORKeyStream(p[:n], p[:n])
	}
	return n, err
}

func (er *EncryptedReader) Close() error {
	return er.reader.Close()
}
