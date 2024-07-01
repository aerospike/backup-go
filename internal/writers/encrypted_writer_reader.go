package writers

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
)

type encryptedWriter struct {
	writer io.WriteCloser
	stream cipher.Stream
}

// NewEncryptedWriter returns a new Writer
// Writes to the returned writer are encrypted with a key and written to w.
func NewEncryptedWriter(w io.WriteCloser, key []byte) (io.WriteCloser, error) {
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

	return &encryptedWriter{
		writer: w,
		stream: stream,
	}, nil
}

func (ew *encryptedWriter) Write(p []byte) (int, error) {
	encrypted := make([]byte, len(p))
	ew.stream.XORKeyStream(encrypted, p)

	return ew.writer.Write(encrypted)
}

func (ew *encryptedWriter) Close() error {
	return ew.writer.Close()
}

type encryptedReader struct {
	reader io.ReadCloser
	stream cipher.Stream
}

// NewEncryptedReader create new reader, decrypting data from underlying reader with a key.
func NewEncryptedReader(r io.ReadCloser, key []byte) (io.ReadCloser, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(r, iv); err != nil {
		return nil, err
	}

	stream := cipher.NewCTR(block, iv)

	return &encryptedReader{
		reader: r,
		stream: stream,
	}, nil
}

func (er *encryptedReader) Read(p []byte) (int, error) {
	n, err := er.reader.Read(p)
	if n > 0 {
		er.stream.XORKeyStream(p[:n], p[:n])
	}

	return n, err
}

func (er *encryptedReader) Close() error {
	return er.reader.Close()
}
