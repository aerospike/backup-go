package writers

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
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

	// Encrypt the IV before writing
	encIv := make([]byte, aes.BlockSize)
	// For compatibility with the original asbackup format
	ctr128SubtractFrom(encIv, iv, 1)
	block.Encrypt(encIv, encIv)

	// Write the IV to the underlying writer
	if _, err := w.Write(encIv); err != nil {
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

// NewEncryptedReader create new reader, decrypting data from underlying reader
// with a key.
func NewEncryptedReader(r io.ReadCloser, key []byte) (io.ReadCloser, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(r, iv); err != nil {
		return nil, err
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
